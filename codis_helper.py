from collections import namedtuple
from email.mime.text import MIMEText
from functools import partial
from kazoo.client import KazooClient
from redis.exceptions import BusyLoadingError

import docker
import re
import json
import logging
import random
import redis
import requests
import smtplib
import socket
import time
import uuid


def _retry_internal(f, exceptions=Exception, tries=5, delay=5, max_delay=None, backoff=1, jitter=(0, 1), logger=None):
    """
    Executes a function and retries it if it failed.
    :param ExceptionToCheck: the exception to check. may be a tuple of exceptions to check
    :param tries: number of times to try (not retry) before giving up
    :param delay: initial delay between retries in seconds
    :param max_delay: the maximum value of delay. default: None (no limit).
    :param backoff: backoff multiplier e.g. value of 2 will double the delay each retry
    :param jitter: extra seconds added to delay between attempts. default: 0.
                   fixed if a number, random if a range tuple (min, max)
    :param logger: logger to use. If None, not print
    :returns: the result of the f function.
    """
    mtries, mdelay = tries, delay
    while mtries > 1:
        try:
            return f()
        except exceptions, e:
            mtries -= 1
            mdelay *= backoff
            if isinstance(jitter, tuple):
                adelay = random.uniform(*jitter)
            else:
                adelay = jitter
            mdelay += adelay
            if max_delay is not None:
                mdelay = min(mdelay, max_delay)
            if logger:
                msg = "[%d/%d] err = %s, retrying in %.2f seconds" % (tries - mtries, tries, str(e), mdelay)
                logger.warning(msg)
            time.sleep(mdelay)
    return f()


def retry(exceptions=Exception, tries=5, delay=5, max_delay=None, backoff=1, jitter=(0, 1), logger=None):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            return _retry_internal(partial(f, *args, **kwargs), exceptions, tries, delay, max_delay, backoff, jitter, logger)
        return f_retry  # true decorator
    return deco_retry


def retry_call(f, fargs=None, fkwargs=None, exceptions=Exception, tries=5, delay=5, max_delay=None, backoff=1, jitter=(0, 1), logger=None):
    args = fargs if fargs else list()
    kwargs = fkwargs if fkwargs else dict()
    return _retry_internal(partial(f, *args, **kwargs), exceptions, tries, delay, max_delay, backoff, jitter, logger)


logging.basicConfig(level=logging.INFO, format='[%(asctime)s][%(levelname)8s][%(filename)s][%(funcName)s][%(lineno)d]: %(message)s')
log = logging.getLogger()

DashboardConfig = namedtuple('DashboardConfig', [
        'version',
        'docker_image',
        'zk_servers',
        'product',
        'host',
        'port',
        'dashboard_args',
        'sentinel_quorum',
    ],
    # verbose=True,
)

ServerConfig = namedtuple('ServerConfig', [
        'version',
        'docker_image',
        'product',
        'group_id',
        'host',
        'port',
        'max_memory',
        'container_memory',
        'container_memory_swap',
        'server_args',
        'allow_metrics_collection',
        'graphite_host',
        'graphite_port',
        'graphite_protocal',
        'graphite_prefix',
    ],
    # verbose=True,
)

ServerGroupConfig = namedtuple('ServerGroupConfig', [
        'version',
        'docker_image',
        'product',
        'group_id',
        'host_port_tuples',
        'max_memory',
        'container_memory',
        'container_memory_swap',
        'server_args',
        'allow_metrics_collection',
        'graphite_host',
        'graphite_port',
        'graphite_protocal',
        'graphite_prefix',
    ],
    # verbose=True,
)

ProxyConfig = namedtuple('ProxyConfig', [
        'version',
        'docker_image',
        'zk_servers',
        'product',
        'host',
        'port',
        'http_port',
        'proxy_args',
        'allow_metrics_collection',
        'graphite_host',
        'graphite_port',
        'graphite_protocal',
        'graphite_prefix',
    ],
    # verbose=True,
)

SentinelConfig = namedtuple('SentinelConfig', [
        'version',
        'docker_image',
        'product',
        'host',
        'port',
        'sentinel_args',
    ],
    # verbose=True,
)

CodisClusterConfig = namedtuple('CodisClusterConfig', [
        'version',
        'docker_image',
        'zk_servers',
        'product',
        'dashboard_host_port_tuples',
        'dashboard_args',
        'server_host_port_tuples',
        'max_memory',
        'container_memory',
        'container_memory_swap',
        'server_args',
        'proxy_host_port_tuples',
        'proxy_args',
        'sentinel_host_port_tuples',
        'sentinel_args',
        'allow_metrics_collection',
        'graphite_host',
        'graphite_port',
        'graphite_protocal',
        'graphite_prefix',
    ],
    # verbose=True,
)


class MailHelper(object):

    @staticmethod
    def send_mail(msg, subject, send_to=None, send_from=None):
        pass


class CodisHelper(object):

    @classmethod
    def ping_server(cls, host, port, max_retry=5):
        for i in xrange(max_retry):
            try:
                if i > 0:
                    time.sleep(3.)
                if redis.StrictRedis(host=host, port=port).ping():
                    return True
            except BusyLoadingError as e:
                log.exception(e)
                return True
            except Exception as e:
                if i == max_retry - 1:
                    log.exception(e)
        return False

    @classmethod
    def confirm_ping_server(cls, host, port, max_retry=10):
        for i in xrange(max_retry):
            try:
                if i > 0:
                    time.sleep(5.)
                if redis.StrictRedis(host=host, port=port).ping():
                    return True
            except Exception as e:
                if i == max_retry - 1:
                    log.exception(e)
        return False

    @classmethod
    def split_host_port(cls, address):
        return address.split(':')[0], int(address.split(':')[1])

    @classmethod
    def get_dashboard_host_port_from_node(cls, zk, path, node):
        if node.isdigit():
            address = zk.get('%s/%s' % (path, node))[0]
        elif ':' in node:
            address = node
        else:
            raise Exception('Invalid dashboard node: %s/%s' % (path, node))
        return cls.split_host_port(address)

    @classmethod
    def _check_server_synced(cls, slave_host, slave_port, master_host=None, master_port=None):
        data = redis.StrictRedis(host=slave_host, port=slave_port).info('replication')
        res = data.get('master_link_status') == 'up' and data.get('master_sync_in_progress') == 0
        if master_host is not None:
            res = res and data.get('master_host') == master_host
        if master_port is not None:
            res = res and data.get('master_port') == master_port
        assert res, '%s:%s <= %s:%d, %s' % (master_host, master_port, slave_host, slave_port, data)

    @classmethod
    def confirm_server_synced(cls, slave_host, slave_port, master_host=None, master_port=None, max_retry=200):
        for i in xrange(max_retry - 1):
            try:
                cls._check_server_synced(slave_host, slave_port, master_host, master_port)
                return True
            except Exception as e:
                #log.warn('confirm_server_synced [%d/%d] %s:%s ==> %s:%s' %(i+1,max_retry,slave_host,slave_port,master_host,master_port))
                time.sleep(5.)
        cls._check_server_synced(slave_host, slave_port, master_host, master_port)
        return True

    @classmethod
    def get_server_replication(cls, host, port):
        """
        [('master' or 'slave', (master_host,master_port), {'slave#num': { 'ip state lag port offset': ..} }, )]
        """
        data = redis.StrictRedis(host=host, port=port).info()
        role = data['role']
        master_host = data.get('master_host')
        master_port = data.get('master_port')
        slaves = {k: v for k, v in data.iteritems() if k.startswith('slave')}
        return role, (master_host, master_port), slaves

    @classmethod
    def get_ip(cls, host_or_ip):
        return host_or_ip if re.match('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', host_or_ip) else socket.gethostbyname(host_or_ip)

_DOCKER_HELPER_MAPPER = {}


class DockerHelper(object):
    AUTH_CONFIG = {'username': 'zihong.lv@gmail.com', 'password': 'bqfoton20170601'}

    def __init__(self, host, base_url, version='auto', **kwargs):
        self.host = host
        self.base_url = base_url
        if docker.version.startswith('2.'):
            self.client = docker.DockerClient(base_url=base_url, version=version, **kwargs)
            self.api_client = self.client.api
        elif docker.version.startswith('1.8.1'):
            self.client = docker.Client(base_url=base_url, version=version, **kwargs)
            self.api_client = self.client
        else:
            raise NotImplementedError('docker version: %s' % (docker.version))
        log.info('Initailizing docker client[%s] = %s %s' % (host, base_url, self.api_client.version()['Version']))

    @staticmethod
    def get_docker_helper(host, base_url=None, **kwargs):
        if host in _DOCKER_HELPER_MAPPER:
            return _DOCKER_HELPER_MAPPER[host]
        ret = None
        if base_url is None:
            base_url = 'tcp://%s:4243' % (host)
        for i in xrange(5):
            try:
                dh = DockerHelper(host, base_url, **kwargs)
                dh.client.info()
            except Exception as e:
                ret = e
            else:
                _DOCKER_HELPER_MAPPER[host] = dh
                return dh
        raise ret

    def get_containers(self, unique_id, filters=None):
        cids = [x['Id'] for x in self.api_client.containers(
            quiet=True,
            all=True,
            filters=filters,
        )]
        ret = list()
        for cid in cids:
            l = self.api_client.inspect_container(cid)['Config']['Env']
            key = ['='.join(s.split('=')[1:]) for s in l if s.split('=')[0] == 'CODIS_UNIQUE_IDENTIFIER'][0]
            if key == unique_id:
                ret.append(cid)
        return ret

    def stop_containers(self, unique_id, filters=None, timeout=1):
        cids = self.get_containers(unique_id, filters)
        for cid in cids:
            self.api_client.stop(container=cid, timeout=timeout)
        return cids


class Dashboard(DashboardConfig):

    def __init__(self, *args, **kwargs):
        super(Dashboard, self).__init__(*args, **kwargs)

    def _get_unique_id(self):
        return '%s,%s,%s,%s' % (self.product, 'dashboard', self.host, self.port)

    def _get_filters(self):
        return {
            'status': ['running', 'paused'],
            'label': ['CODIS_GROUP_NAME=%s' % (self.product), 'CODIS_COMPONENT_TYPE=dashboard', ],
        }

    def run(self):
        log.info('Running dashboard: %s %d' % (self.host, self.port))
        dh = DockerHelper.get_docker_helper(self.host)
        pull_log = dh.api_client.pull(self.docker_image, auth_config=dh.AUTH_CONFIG)

        host_config = dh.api_client.create_host_config(
            network_mode='host',
        )
        environment = {
            'CODIS_GROUP_NAME': self.product,
            'CODIS_COMPONENT_TYPE': 'dashboard',
            'CODIS_ZK': self.zk_servers,
            'CODIS_PRODUCT': self.product,
            'CODIS_DASHBOARD_PORT': self.port,
            'CODIS_SENTINEL_QUORUM': self.sentinel_quorum,
            'CODIS_DASHBOARD_HOST_HOST': self.host,
            'CODIS_DASHBOARD_HOST_PORT': self.port,
            'CODIS_DASHBOARD_ARGS': self.dashboard_args,
            'CODIS_UNIQUE_IDENTIFIER': self._get_unique_id(),
        }
        labels = {
            'CODIS_GROUP_NAME': self.product,
            'CODIS_COMPONENT_TYPE': 'dashboard',
        }
        container = dh.api_client.create_container(
            self.docker_image,
            command='bash dashboard.sh',
            detach=True,
            environment=environment,
            host_config=host_config,
            labels=labels,
        )
        dh.api_client.start(container=container.get('Id'))
        MailHelper.send_mail(msg='%s' % (self._asdict()), subject='[Codis %s] Dashboard Creation' % (self.product))
        return container

    def confirm_ping(self, retry=10):
        def dummy_caller():
            assert requests.get('http://%s:%d' % (self.host, self.port)).status_code == 200
        try:
            retry_call(dummy_caller, tries=retry)
        except:
            return False
        return True

    def is_alive(self, zk, retry=10):
        if self.version not in {'2.0', '3.1'}:
            raise NotImplementedError
        if self.version == '2.0':
            path = '/zk/codis/db_%s/dashboard' % (self.product)
            if not zk.exists(path):
                return False
            dashboard_nodes = zk.get_children(path)
            existed = False
            for node in dashboard_nodes:
                if node.isdigit():
                    address = zk.get('%s/%s' % (path, node))[0]
                elif ':' in node:
                    address = node
                else:
                    raise Exception('Invalid dashboard node: %s/%s' % (path, node))
                if address == '%s:%d' % (self.host, self.port):
                    existed = True
                    break
            if not existed:
                return False
        elif self.version == '3.1':
            path = '/codis3/%s/topom' % (self.product)
            if not zk.exists(path):
                return False
            data = json.loads(zk.get(path)[0])
            if data['admin_addr'] != '%s:%d' % (self.host, self.port):
                return False
        if self.confirm_ping(retry):
            return True
        dh = DockerHelper.get_docker_helper(self.host)
        cids = dh.get_containers(self._get_unique_id(), self._get_filters())
        if len(cids) == 1:
            raise Exception("dashboard is alive but cannot ping: %s" % (self))
        return False

    def stop_all(self):
        log.info('Stopping dashboard: %s %d' % (self.host, self.port))
        dh = DockerHelper.get_docker_helper(self.host)
        return len(dh.stop_containers(self._get_unique_id(), self._get_filters()))

    def get_cid(self):
        dh = DockerHelper.get_docker_helper(self.host)
        cids = dh.get_containers(self._get_unique_id(), self._get_filters())
        assert len(cids) == 1, 'dashboard [%s] #cids: [%s]' % (self, cids)
        return cids[0]

    def exec_command(self, cmd):
        dh = DockerHelper.get_docker_helper(self.host)
        cid = self.get_cid()
        d = dh.api_client.exec_create(cid, cmd)
        s = dh.api_client.exec_start(d['Id'])
        return s


class Sentinel(SentinelConfig):

    def __init__(self, *args, **kwargs):
        super(Sentinel, self).__init__(*args, **kwargs)
        if self.version == '3.1':
            pass
        else:
            raise NotImplementedError

    def _get_unique_id(self):
        return '%s,%s,%s,%s' % (self.product, 'sentinel', self.host, self.port)

    def _get_filters(self):
        return {
            'status': ['running', 'paused'],
            'label': ['CODIS_GROUP_NAME=%s' % (self.product), 'CODIS_COMPONENT_TYPE=sentinel', ],
        }

    def run(self):
        log.info('Running sentinel: %s %d' % (self.host, self.port))
        dh = DockerHelper.get_docker_helper(self.host)
        pull_log = dh.api_client.pull(self.docker_image, auth_config=dh.AUTH_CONFIG)

        host_config = dh.api_client.create_host_config(
            network_mode='host',
        )
        environment = {
            'CODIS_GROUP_NAME': self.product,
            'CODIS_COMPONENT_TYPE': 'sentinel',
            'CODIS_UNIQUE_IDENTIFIER': self._get_unique_id(),
            'CODIS_SENTINEL_PORT': self.port,
            'CODIS_SENTINEL_HOST_HOST': self.host,
            'CODIS_SENTINEL_HOST_PORT': self.port,
            'CODIS_SENTINEL_ARGS': self.sentinel_args,
        }
        labels = {
            'CODIS_GROUP_NAME': self.product,
            'CODIS_COMPONENT_TYPE': 'sentinel',
        }
        container = dh.api_client.create_container(
            self.docker_image,
            command='bash sentinel.sh',
            detach=True,
            environment=environment,
            host_config=host_config,
            labels=labels,
        )
        dh.api_client.start(container=container.get('Id'))
        MailHelper.send_mail(msg='%s' % (self._asdict()), subject='[Codis %s] Sentinel Creation' % (self.product))
        return container

    def is_alive(self, zk, retry=10):
        if self.version not in {'3.1'}:
            raise NotImplementedError
        path = '/codis3/%s/sentinel' % (self.product)
        if not zk.exists(path):
            return False
        data = json.loads(zk.get(path)[0])
        if '%s:%d' % (self.host, self.port) not in data['servers']:
            return False
        if CodisHelper.confirm_ping_server(self.host, self.port, retry):
            return True
        dh = DockerHelper.get_docker_helper(self.host)
        cids = dh.get_containers(self._get_unique_id(), self._get_filters())
        if len(cids) == 1:
            raise Exception("sentinel is alive but cannot ping: %s" % (self))
        return False

    def stop_all(self):
        log.info('Stopping sentinel: %s %d' % (self.host, self.port))
        dh = DockerHelper.get_docker_helper(self.host)
        return len(dh.stop_containers(self._get_unique_id(), self._get_filters()))

    def get_cid(self):
        dh = DockerHelper.get_docker_helper(self.host)
        cids = dh.get_containers(self._get_unique_id(), self._get_filters())
        assert len(cids) == 1, 'sentinel [%s] #cids: [%s]' % (self, cids)
        return cids[0]

    def exec_command(self, cmd):
        dh = DockerHelper.get_docker_helper(self.host)
        cid = self.get_cid()
        d = dh.api_client.exec_create(cid, cmd)
        s = dh.api_client.exec_start(d['Id'])
        return s


class Server(ServerConfig):

    def __init__(self, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)

    def _get_unique_id(self):
        return '%s,%s,%s,%s' % (self.product, 'server=%s' % (self.group_id), self.host, self.port)

    def _get_filters(self):
        return {
            'status': ['running', 'paused'],
            'label': ['CODIS_GROUP_NAME=%s' % (self.product), 'CODIS_COMPONENT_TYPE=server', ],
        }

    def run(self):
        log.info('Running server: %s %d' % (self.host, self.port))
        dh = DockerHelper.get_docker_helper(self.host)
        pull_log = dh.api_client.pull(self.docker_image, auth_config=dh.AUTH_CONFIG)

        host_config = dh.api_client.create_host_config(
            network_mode='host',
            mem_limit=self.container_memory,
            memswap_limit=self.container_memory_swap,
        )
        environment = {
            'CODIS_GROUP_NAME': self.product,
            'CODIS_COMPONENT_TYPE': 'server',
            'CODIS_PRODUCT': self.product,
            'ALLOW_METRICS_COLLECTION': self.allow_metrics_collection,
            'COLLECTD_HOST': self.host,
            'GRAPHITE_HOST': self.graphite_host,
            'GRAPHITE_PORT': self.graphite_port,
            'GRAPHITE_PROTOCOL': self.graphite_protocal,
            'GRAPHITE_PREFIX': self.graphite_prefix,
            'CODIS_SERVER_PORT': self.port,
            'CODIS_SERVER_HOST_HOST': self.host,
            'CODIS_SERVER_HOST_PORT': self.port,
            'CODIS_SERVER_MAX_MEMORY': self.max_memory,
            'CODIS_SERVER_ARGS': self.server_args,
            'CODIS_UNIQUE_IDENTIFIER': self._get_unique_id(),
        }
        labels = {
            'CODIS_GROUP_NAME': self.product,
            'CODIS_COMPONENT_TYPE': 'server',
        }
        container = dh.api_client.create_container(
            self.docker_image,
            command='bash server.sh',
            detach=True,
            environment=environment,
            host_config=host_config,
            labels=labels,
        )
        dh.api_client.start(container=container.get('Id'))
        MailHelper.send_mail(msg='%s' % (self._asdict()), subject='[Codis %s] Server Creation' % (self.product))
        assert CodisHelper.confirm_ping_server(self.host, self.port), self
        return container

    def is_alive(self, zk, retry=10):
        if self.version not in {'2.0', '3.1'}:
            return False
        if self.version == '2.0':
            path = '/zk/codis/db_%s/servers/group_%d/%s:%d' % (self.product, self.group_id, self.host, self.port)
            if not zk.exists(path):
                return False
            data = json.loads(zk.get(path)[0])
            if data['type'].lower() not in ['master', 'slave']:
                return False
        elif self.version == '3.1':
            path = '/codis3/%s/group/group-%04d' % (self.product, self.group_id)
            if not zk.exists(path):
                return False
            data = json.loads(zk.get(path)[0])
            servers = [x['server'] for x in data['servers']]
            if '%s:%d' % (self.host, self.port) not in servers:
                return False
        if CodisHelper.confirm_ping_server(self.host, self.port, retry):
            return True
        dh = DockerHelper.get_docker_helper(self.host)
        cids = dh.get_containers(self._get_unique_id(), self._get_filters())
        if len(cids) == 1:
            raise Exception("server is alive but cannot ping: %s" % (self))
        return False

    def stop_all(self):
        log.info('Stopping server: %s %d' % (self.host, self.port))
        dh = DockerHelper.get_docker_helper(self.host)
        return len(dh.stop_containers(self._get_unique_id(), self._get_filters()))


class ServerGroup(ServerGroupConfig):

    def __init__(self, *args, **kwargs):
        super(ServerGroup, self).__init__(*args, **kwargs)
        host_set = {x[0] for x in self.host_port_tuples}
        #assert len(host_set) == len(self.host_port_tuples), 'Duplicated host: %s' % (self.host_port_tuples)
        self.servers = {
            x: Server(
                self.version,
                self.docker_image,
                self.product,
                self.group_id,
                x[0],
                x[1],
                self.max_memory,
                self.container_memory,
                self.container_memory_swap,
                self.server_args,
                self.allow_metrics_collection,
                self.graphite_host,
                self.graphite_port,
                self.graphite_protocal,
                self.graphite_prefix,
            ) for x in self.host_port_tuples
        }

    @classmethod
    def from_slot(cls, group_id, server_group_number):
        return (group_id - 1) * 1024 / server_group_number

    @classmethod
    def to_slot(cls, group_id, server_group_number):
        return group_id * 1024 / server_group_number - 1

    def get_slot_range(self, server_group_number):
        return self.from_slot(self.group_id, server_group_number), self.to_slot(self.group_id, server_group_number)

    def is_normal(self):
        raise NotImplementedError


class Proxy(ProxyConfig):

    def __init__(self, *args, **kwargs):
        super(Proxy, self).__init__(*args, **kwargs)

    def _get_unique_id(self):
        return '%s,%s,%s,%s' % (self.product, 'proxy', self.host, self.port)

    def _get_filters(self):
        return {
            'status': ['running', 'paused'],
            'label': ['CODIS_GROUP_NAME=%s' % (self.product), 'CODIS_COMPONENT_TYPE=proxy', ],
        }

    def run(self):
        log.info('Running proxy: %s %d %d' % (self.host, self.port, self.http_port))
        dh = DockerHelper.get_docker_helper(self.host)
        pull_log = dh.api_client.pull(self.docker_image, auth_config=dh.AUTH_CONFIG)

        host_config = dh.api_client.create_host_config(
            network_mode='host',
            restart_policy=dict(
                Name='on-failure',
                # MaximumRetryCount=10,
            ),
        )
        environment = {
            'CODIS_GROUP_NAME': self.product,
            'CODIS_COMPONENT_TYPE': 'proxy',
            'CODIS_ZK': self.zk_servers,
            'CODIS_PRODUCT': self.product,
            'CODIS_PROXY_ID': 'proxy_%s_%s' % (self.host, self.port),
            'ALLOW_METRICS_COLLECTION': self.allow_metrics_collection,
            'COLLECTD_HOST': self.host,
            'GRAPHITE_HOST': self.graphite_host,
            'GRAPHITE_PORT': self.graphite_port,
            'GRAPHITE_PROTOCOL': self.graphite_protocal,
            'GRAPHITE_PREFIX': self.graphite_prefix,
            'CODIS_PROXY_HOST': '0.0.0.0',
            'CODIS_PROXY_PORT': self.port,
            'CODIS_PROXY_HTTP_PORT': self.http_port,
            'CODIS_PROXY_HOST_HOST': self.host,
            'CODIS_PROXY_HOST_PORT': self.port,
            'CODIS_PROXY_HOST_HTTP_PORT': self.http_port,
            'CODIS_JODIS_NAME': 'zookeeper',
            'CODIS_JODIS_ADDR': self.zk_servers,
            'CODIS_JODIS_COMPATIBLE': 'true',
            'CODIS_PROXY_ARGS': self.proxy_args,
            'CODIS_UNIQUE_IDENTIFIER': self._get_unique_id(),
        }
        labels = {
            'CODIS_GROUP_NAME': self.product,
            'CODIS_COMPONENT_TYPE': 'proxy',
        }
        container = dh.api_client.create_container(
            self.docker_image,
            command='bash proxy.sh',
            detach=True,
            environment=environment,
            host_config=host_config,
            labels=labels,
        )
        dh.api_client.start(container=container.get('Id'))
        MailHelper.send_mail(msg='%s' % (self._asdict()), subject='[Codis %s] Proxy Creation' % (self.product))
        return container

    def is_alive(self, zk, retry=10):
        if self.version not in {'2.0', '3.1'}:
            raise NotImplementedError
        if self.version == '2.0':
            path = '/zk/codis/db_%s/proxy/proxy_%s_%d' % (self.product, self.host, self.port)
            if not zk.exists(path):
                return False
            data = json.loads(zk.get(path)[0])
            if data['state'].lower() != 'online':
                return False
        elif self.version == '3.1':
            path = '/codis3/%s/proxy' % (self.product)
            if not zk.exists(path):
                return False
            proxy_nodes = zk.get_children(path)
            existed = False
            for node in proxy_nodes:
                data = json.loads(zk.get('%s/%s' % (path, node))[0])
                proxy_addr = data['proxy_addr']
                admin_addr = data['proxy_addr']
                if (proxy_addr == '%s:%d' % (self.host, self.port)) and (admin_addr == '%s:%d' % (self.host, self.port)):
                    existed = True
                    break
            if not existed:
                return False
        if CodisHelper.confirm_ping_server(self.host, self.port, retry):
            return True
        dh = DockerHelper.get_docker_helper(self.host)
        cids = dh.get_containers(self._get_unique_id(), self._get_filters())
        if len(cids) == 1:
            raise Exception("proxy is alive but cannot ping: %s" % (self))
        return False

    def stop_all(self):
        log.info('Stopping proxy: %s %d %d' % (self.host, self.port, self.http_port))
        dh = DockerHelper.get_docker_helper(self.host)
        return len(dh.stop_containers(self._get_unique_id(), self._get_filters()))


class CodisCluster(CodisClusterConfig):

    def __init__(self, *args, **kwargs):
        super(CodisCluster, self).__init__(*args, **kwargs)
        assert self.version in ['3.1', '2.0'], 'version = %s' % (self.version)
        if self.version == '3.1':
            assert len(self.sentinel_host_port_tuples) % 2 == 1, '#sentinel_host_port_tuples = %d' % (len(self.sentinel_host_port_tuples))
            assert len(self.dashboard_host_port_tuples) == 1, '#dashboard_host_port_tuples = %d' % (len(self.dashboard_host_port_tuples))
        elif self.version == '2.0':
            assert len(self.sentinel_host_port_tuples) == 0, '#sentinel_host_port_tuples = %d' % (len(self.sentinel_host_port_tuples))
        self.dashboards = {
            x: Dashboard(
                self.version,
                self.docker_image,
                self.zk_servers,
                self.product,
                x[0],
                x[1],
                self.dashboard_args,
                (len(self.sentinel_host_port_tuples) + 1) / 2,
            ) for x in self.dashboard_host_port_tuples
        }
        self.sentinels = {
            x: Sentinel(
                self.version,
                self.docker_image,
                self.product,
                x[0],
                x[1],
                self.sentinel_args,
            ) for x in self.sentinel_host_port_tuples
        }
        self.server_groups = {
            str(i + 1): ServerGroup(
                self.version,
                self.docker_image,
                self.product,
                i + 1,
                host_port_tuples,
                self.max_memory,
                self.container_memory,
                self.container_memory_swap,
                self.server_args,
                self.allow_metrics_collection,
                self.graphite_host,
                self.graphite_port,
                self.graphite_protocal,
                self.graphite_prefix,
            ) for i, host_port_tuples in enumerate(self.server_host_port_tuples)
        }
        self.proxies = {
            x: Proxy(
                self.version,
                self.docker_image,
                self.zk_servers,
                self.product,
                x[0],
                x[1],
                x[2],
                self.proxy_args,
                self.allow_metrics_collection,
                self.graphite_host,
                self.graphite_port,
                self.graphite_protocal,
                self.graphite_prefix,
            ) for x in self.proxy_host_port_tuples
        }
        self.zk = KazooClient(hosts=self.zk_servers)
        self.zk.start()

    @property
    def server_group_number(self):
        return len(self.server_host_port_tuples)

    def zk_root_path(self, overwrite_version=None):
        version = overwrite_version or self.version
        if version == '3.1':
            ret = '/codis3/%s' % (self.product)
        elif version == '2.0':
            ret = '/zk/codis/db_%s' % (self.product)
        else:
            raise NotImplementedError
        return ret

    def remove_zk_root(self, overwrite_version=None):
        log.info('removing zk root')
        self.zk.delete(self.zk_root_path(overwrite_version), recursive=True)

    def get_all_dashboard_host_ports(self, limit=None):
        root_path = self.zk_root_path()
        res = list()
        if self.version == '3.1':
            data = json.loads(self.zk.get('%s/topom' % (root_path))[0])
            res.append(data['admin_addr'])
        elif self.version == '2.0':
            path = '%s/dashboard' % (root_path)
            dashboard_nodes = sorted(self.zk.get_children(path))
            if limit is not None:
                dashboard_nodes = dashboard_nodes[:limit]
            for node in dashboard_nodes:
                if node.isdigit():
                    address = self.zk.get('%s/%s' % (path, node))[0]
                elif ':' in node:
                    address = node
                else:
                    raise Exception('Invalid dashboard node: %s/%s' % (path, node))
                res.append(address)
        else:
            raise NotImplementedError
        return [(address.split(':')[0], int(address.split(':')[1])) for address in res]

    def get_dashboard_host_port(self):
        return self.get_all_dashboard_host_ports(limit=1)[0]

    def get_slots_count(self):
        if self.version == '2.0':
            path = '/zk/codis/db_%s/slots' % (self.product)
            if not self.zk.exists(path):
                return 0
            tmp = self.zk.get(path)
            return tmp[1].children_count
        else:
            raise NotImplementedError

    def slot_init(self, is_force=False):
        if self.version == '2.0':
            dashboard_host, dashboard_port = self.get_dashboard_host_port()
            url = 'http://%s:%d/api/slots/init' % (dashboard_host, dashboard_port)
            if is_force:
                url += "?is_force=1"
            res = requests.post(url)
            assert((res.status_code == 200) or res.content.startswith('slots already initialized'))
        else:
            raise NotImplementedError

    def ensure_slot_init(self, retry=30):
        if self.version == '2.0':
            count = self.get_slots_count()
            if count != 1024:
                self.slot_init()
                for i in xrange(retry):
                    time.sleep(5.)
                    count = self.get_slots_count()
                    if count != 1024:
                        log.warn('slot initing, iter = [%d/%d], #slot = %d' % (i + 1, retry, count))
                    else:
                        break
            return count
        else:
            raise NotImplementedError

    def get_proxy_list_from_zk(self):
        if self.version == '2.0':
            path = '/zk/codis/db_%s/proxy' % (self.product)
            ret = list()
            if not self.zk.exists(path):
                return ret
            nodes = self.zk.get_children(path)
            for node in nodes:
                data = json.loads(self.zk.get('%s/%s' % (path, node))[0])
                host, port = CodisHelper.split_host_port(data['addr'])
                admin_host, admin_port = CodisHelper.split_host_port(data['debug_var_addr'])
                #assert host==admin_host
                ret.append((host, port, admin_port))
            return ret
        else:
            raise NotImplementedError

    def clean_proxy_fence(self):
        log.info('Cleaning proxy fence')
        if self.version == '2.0':
            path = '/zk/codis/db_%s/fence' % (self.product)
            if not self.zk.exists(path):
                log.debug('zk path not existed: %s' % (path))
                return 0
            proxy_set = set([(x[0], x[1]) for x in self.get_proxy_list_from_zk()])
            nodes = self.zk.get_children(path)
            non_existed_nodes = [node for node in nodes if CodisHelper.split_host_port(node) not in proxy_set]
            for node in nodes:
                fence_path = '%s/%s' % (path, node)
                log.debug('deleting fence_path = %s' % (fence_path))
                self.zk.delete(fence_path)
            return len(non_existed_nodes)
        else:
            raise NotImplementedError

    def get_server_group_count(self):
        if self.version == '3.1':
            tmp = self.zk.exists('/codis3/%s/group' % (self.product))
        elif self.version == '2.0':
            tmp = self.zk.exists('/zk/codis/db_%s/servers' % (self.product))
        else:
            raise NotImplementedError
        return -1 if tmp is None else tmp.children_count

    def server_group_exists(self, group_id):
        if self.version == '2.0':
            path = '/zk/codis/db_%s/servers' % (self.product)
            group_name = 'group_%d' % (group_id)
        elif self.version == '3.1':
            path = '/codis3/%s/group' % (self.product)
            group_name = 'group-%04d' % (group_id)
        else:
            raise NotImplementedError
        if not self.zk.exists(path):
            return False
        children = self.zk.get_children(path)
        return group_name in children

    @classmethod
    def _range_intersection(cls, from1, to1, from2, to2):
        from_0 = max(from1, from2)
        to_0 = min(to1, to2)
        return None if from_0 > to_0 else from_0, to_0

    def get_topom_stats(self):
        if self.version == '3.1':
            dashboard_host, dashboard_port = self.get_dashboard_host_port()
            res = requests.get(
                'http://%s:%d/topom/stats' % (dashboard_host, dashboard_port),
            )
            assert res.status_code == 200, '[%d][%s]' % (res.status_code, res.content)
            return json.loads(res.content)
        else:
            raise NotImplementedError

    def sentinel_resync(self, dashboard=None):
        if self.version == '3.1':
            dashboard = dashboard or self.dashboards.values()[0]
            cmd = 'codis-admin --dashboard=%s:%d --sentinel-resync' % (dashboard.host, dashboard.port)
            s = dashboard.exec_command(cmd)
            assert s == '', 'exec: %s => %s' % (cmd, s)
        else:
            raise NotImplementedError

    def add_server_group(self, group_id):
        if self.version == '2.0':
            dashboard_host, dashboard_port = self.get_dashboard_host_port()
            res = requests.put(
                'http://%s:%d/api/server_groups' % (dashboard_host, dashboard_port),
                data='{"id":%d}' % (group_id),
            )
            assert ((res.status_code == 200) or res.content.startswith('group already exists')), '[%d][%s]' % (res.status_code, res.content)
        elif self.version == '3.1':
            dashboard = self.dashboards.values()[0]
            cmd = 'codis-admin --dashboard=%s:%d --create-group --gid=%d' % (dashboard.host, dashboard.port, group_id)
            s = dashboard.exec_command(cmd)
            assert s == '', 'exec: %s => %s' % (cmd, s)
            self.sentinel_resync(dashboard)
        else:
            raise NotImplementedError

    def server_sync(self, host, port, dashboard=None, group_id=None):
        if self.version == '3.1':
            dashboard = dashboard or self.dashboards.values()[0]
            cmd = 'codis-admin --dashboard=%s:%d --sync-action --create --addr=%s:%d' % (dashboard.host, dashboard.port, host, port)
            s = dashboard.exec_command(cmd)
            assert s == '', 'exec: %s => %s' % (cmd, s)
        else:
            raise NotImplementedError

    def add_server_to_group(self, group_id, host, port, sync=True, sentinel_sync=True):
        log.info('Adding server [%s:%d] to group %d' % (host, port, group_id))
        assert(CodisHelper.confirm_ping_server(host, port))
        if self.version == '2.0':
            assert sync
            dashboard_host, dashboard_port = self.get_dashboard_host_port()
            res = requests.put(
                'http://%s:%d/api/server_group/%d/addServer' % (dashboard_host, dashboard_port, group_id),
                data='{"addr":"%s:%d","type":"slave","group_id":%d}' % (host, port, group_id),
            )
            assert ((res.status_code == 200) or res.content.startswith('can not slave of itself')), '[%d][%s]' % (res.status_code, res.content)
        elif self.version == '3.1':
            dashboard = self.dashboards.values()[0]
            cmd = 'codis-admin --dashboard=%s:%d --group-add --gid=%d --addr=%s:%d' % (dashboard.host, dashboard.port, group_id, host, port)
            s = dashboard.exec_command(cmd)
            assert s == '' or 'already exists' in s, 'exec: %s => %s' % (cmd, s)
            if sync:
                self.server_sync(host, port, dashboard)
            if sentinel_sync:
                self.sentinel_resync(dashboard)
        else:
            raise NotImplementedError

    def get_server_group(self, group_id):
        """
        [(host, port, 'master' or 'slave' or 'offline')]
        """
        if self.version == '2.0':
            path = '/zk/codis/db_%s/servers/group_%s' % (self.product, group_id)
            servers = self.zk.get_children(path)
            res = []
            for x in servers:
                data = json.loads(self.zk.get('%s/%s' % (path, x))[0])
                res.append((data['addr'].split(':')[0], int(data['addr'].split(':')[1]), data['type'].lower()))
            master_servers = [x[:2] for x in res if x[2] == 'master']
            assert len(master_servers) == 1, 'Group %d: %s' % (group_id, master_servers)
            t0, t1, slaves = CodisHelper.get_server_replication(master_servers[0][0], master_servers[0][1])
            slave_ip_port_sets = {(CodisHelper.get_ip(slave['ip']), slave['port']) for slave in slaves.values()}
            slave_servers = list()
            other_servers = list()
            for x in res:
                key = (CodisHelper.get_ip(x[0]), x[1])
                if x[2] == 'slave' and key in slave_ip_port_sets:
                    slave_servers.append(x[:2])
                elif x[2] != 'master':
                    other_servers.append(x[:2])
            return master_servers, slave_servers, other_servers
        elif self.version == '3.1':
            path = '/codis3/%s/group/group-%04d' % (self.product, group_id)
            data = json.loads(self.zk.get(path)[0])
            master_servers = [CodisHelper.split_host_port(x['server']) for x in data['servers'][:1]]
            slave_servers = [CodisHelper.split_host_port(x['server']) for x in data['servers'][1:]]
            other_servers = list()
            return master_servers, slave_servers, other_servers
        else:
            raise NotImplementedError

    def promote_server_in_group(self, group_id, host, port, sync_server=True):
        log.info('Promoting server in group %d, [%s:%d]' % (group_id, host, port))
        if self.version == '2.0':
            assert CodisHelper.ping_server(host=host, port=port)
            dashboard_host, dashboard_port = self.get_dashboard_host_port()

            def dummy_caller():
                res = requests.post(
                    'http://%s:%d/api/server_group/%d/promote' % (dashboard_host, dashboard_port, group_id),
                    json={
                        "addr": "%s:%d" % (host, port),
                        "group_id": group_id,
                        "type": "slave",
                    },
                )
                assert res.status_code == 200, '[%d][%s]' % (res.status_code, res.content)
            retry_call(dummy_caller, fargs=None, fkwargs=None, exceptions=Exception, tries=5, delay=5, max_delay=None, backoff=1, jitter=(0, 1), logger=log)
            # TODO: maintain
        elif self.version == '3.1':
            master_servers, slave_servers, other_servers = self.get_server_group(group_id)
            if (host, port) in master_servers:
                return
            dashboard = self.dashboards.values()[0]
            cmd = 'codis-admin --dashboard=%s:%d --promote-server --gid=%d --addr=%s:%d' % (dashboard.host, dashboard.port, group_id, host, port)
            s = dashboard.exec_command(cmd)
            assert '==>' in s, 'exec: %s => %s' % (cmd, s)
            if sync_server:
                for host_port in master_servers + slave_servers:
                    self.server_sync(host_port[0], host_port[1], dashboard)
            self.sentinel_resync(dashboard)
        else:
            raise NotImplementedError

    def replace_servers_in_group(self, group_id):
        log.info('Replacing servers in group %d' % (group_id))
        if self.version not in {'2.0', '3.1'}:
            raise NotImplementedError

        server_group = self.server_groups[str(group_id)]
        new_master_host_port = server_group.host_port_tuples[0]
        new_master_server = server_group.servers[new_master_host_port]
        new_master_host, new_master_port = new_master_host_port

        master_servers, slave_servers, other_servers = self.get_server_group(group_id)
        old_master_host, old_master_port = master_servers[0]

        if (new_master_host_port not in slave_servers) and (new_master_host_port not in master_servers):
            new_master_server.stop_all()
            new_master_server.run()
            redis.StrictRedis(host=new_master_host, port=new_master_port).slaveof(host=old_master_host, port=old_master_port)
            CodisHelper.confirm_server_synced(new_master_host, new_master_port, old_master_host, old_master_port)
            # TODO: distinguish BGSAVE hang and actual down
            self.promote_server_in_group(group_id, old_master_host, old_master_port)
            for new_slave_host_port in server_group.host_port_tuples[1:]:
                if (new_slave_host_port not in slave_servers) and (new_slave_host_port not in master_servers):
                    new_slave_server = server_group.servers[new_slave_host_port]
                    new_slave_host, new_slave_port = new_slave_host_port
                    new_slave_server.stop_all()
                    new_slave_server.run()
                    redis.StrictRedis(host=new_slave_host, port=new_slave_port).slaveof(host=new_master_host, port=new_master_port)
                    CodisHelper.confirm_server_synced(new_slave_host, new_slave_port, new_master_host, new_master_port)
                    self.promote_server_in_group(group_id, old_master_host, old_master_port)

        if new_master_host_port not in master_servers:
            self.add_server_to_group(group_id, new_master_host, new_master_port)
            self.promote_server_in_group(group_id, new_master_host, new_master_port)
            # TODO: v3.1 group sync

        for new_slave_host_port in server_group.host_port_tuples[1:]:
            new_slave_server = server_group.servers[new_slave_host_port]
            new_slave_host, new_slave_port = new_slave_host_port
            if (new_slave_host_port not in slave_servers) and (new_slave_host_port not in master_servers):
                self.add_server_to_group(group_id, new_slave_host, new_slave_port)
            else:
                if self.version == '3.1':
                    self.server_sync(new_slave_host_port, new_slave_port)
                elif self.version == '2.0':
                    redis.StrictRedis(host=new_slave_host, port=new_slave_port).slaveof(host=new_master_host, port=new_master_port)

    def restore_servers_in_group(self, old_cluster, group_id):
        log.info('Restoring servers in group [%d], according to cluster [%s:%s]' % (group_id, old_cluster.product, old_cluster.version))
        if self.version == '3.1':
            if not self.server_group_exists(group_id):
                self.add_server_group(group_id)
            master_servers, slave_servers, other_servers = old_cluster.get_server_group(group_id)
            for host, port in master_servers:
                self.add_server_to_group(group_id, host, port)
            for host, port in slave_servers:
                self.add_server_to_group(group_id, host, port)
            slot_group_dict = old_cluster.get_slot_group_dict()
            slots = [int(slot_id) for slot_id, gid in slot_group_dict.iteritems() if gid == group_id]
            ranges = self._merge_to_ranges(slots)
            for from_slot, to_slot in ranges:
                self.create_slot_range(from_slot, to_slot, group_id)
            return len(master_servers) + len(slave_servers), len(slots)
        else:
            raise NotImplementedError

    def remove_server_from_group(self, group_id, host, port, sync_sentinel=True):
        log.info('Removing server [%s:%d] from group %d' % (host, port, group_id))
        if self.version == '2.0':
            dashboard_host, dashboard_port = self.get_dashboard_host_port()

            def dummy_caller():
                res = requests.put(
                    'http://%s:%d/api/server_group/%d/removeServer' % (dashboard_host, dashboard_port, group_id),
                    json={
                        "addr": "%s:%d" % (host, port),
                        "group_id": group_id,
                        "type": "slave",
                    },
                )
                assert res.status_code == 200, '[%d][%s]' % (res.status_code, res.content)
            retry_call(dummy_caller, tries=2, logger=log)
        elif self.version == '3.1':
            dashboard = self.dashboards.values()[0]
            cmd = 'codis-admin --dashboard=%s:%d --group-del --gid=%d --addr=%s:%d' % (dashboard.host, dashboard.port, group_id, host, port)
            s = dashboard.exec_command(cmd)
            assert s == '', 'exec: %s => %s' % (cmd, s)
            assert redis.StrictRedis(host=host, port=port).slaveof()
            if sync_sentinel:
                self.sentinel_resync(dashboard)
        else:
            raise NotImplementedError

    def strip_server_group(self, group_id, sync_sentinel=True):
        log.info('Striping server_group %d' % (group_id))
        if self.version not in {'2.0', '3.1'}:
            raise NotImplementedError
        master_servers, slave_servers, other_servers = self.get_server_group(group_id)
        if len(master_servers) != 1:
            log.warn('Group %d, len(master_servers) = %d != 1' % (group_id, len(master_servers)))
            return False
        if not CodisHelper.ping_server(host=master_servers[0][0], port=master_servers[0][1]):
            log.warn('Group %d, ping master failed %s:%d' % (group_id, master_servers[0][0], master_servers[0][1]))
            return False
        for host, port in slave_servers + other_servers:
            if (host, port) not in self.server_groups[str(group_id)].host_port_tuples:
                self.remove_server_from_group(group_id, host, port)
        if self.version == '3.1':
            self.sentinel_resync()

    @classmethod
    def _merge_to_ranges(cls, slots):
        slots_sorted = sorted(slots)
        ret = list()
        res_from = None
        res_to = None
        for i, x in enumerate(slots_sorted):
            if (i == 0) or (slots_sorted[i - 1] != x - 1):
                # new range
                if res_from is not None:
                    ret.append((res_from, res_to))
                res_from = x
            res_to = x
        if res_from is not None:
            ret.append((res_from, res_to))
        return ret

    def get_offline_slot_ranges(self):
        slots = list()
        if self.version == '2.0':
            dashboard_host, dashboard_port = self.get_dashboard_host_port()
            res = requests.get(
                'http://%s:%d/api/slots' % (dashboard_host, dashboard_port),
            )
            assert res.status_code == 200, '[%d][%s]' % (res.status_code, res.content)
            data = json.loads(res.content)
            for x in data:
                if x['group_id'] <= 0:
                    assert x['state']['status'] == 'offline', x
                    slots.append(x['id'])
        elif self.version == '3.1':
            data = self.get_topom_stats()
            for x in data['slots']:
                if x['group_id'] <= 0:
                    slots.append(x['id'])
        else:
            raise NotImplementedError
        return self._merge_to_ranges(slots)

    def create_slot_range(self, from_slot, to_slot, group_id):
        log.info('creating slot_range: %d %d %d' % (from_slot, to_slot, group_id))
        if self.version == '2.0':
            dashboard_host, dashboard_port = self.get_dashboard_host_port()
            res = requests.post(
                'http://%s:%d/api/slot' % (dashboard_host, dashboard_port),
                data='{"from":%d,"to":%d,"new_group":%d}' % (from_slot, to_slot, group_id),
            )
            assert res.status_code == 200, '[%d][%s]' % (res.status_code, res.content)
        elif self.version == '3.1':
            dashboard = self.dashboards.values()[0]
            cmd = 'codis-admin --dashboard=%s:%d --slot-action --create-range --beg=%d --end=%d --gid=%d' % (dashboard.host, dashboard.port, from_slot, to_slot, group_id)
            s = dashboard.exec_command(cmd)
            assert s == '', 'exec: %s => %s' % (cmd, s)
        else:
            raise NotImplementedError

    def add_sentinel(self, host, port):
        if self.version == '3.1':
            dashboard = self.dashboards.values()[0]
            cmd = 'codis-admin --dashboard=%s:%d --sentinel-add --addr=%s:%d' % (dashboard.host, dashboard.port, host, port)
            s = dashboard.exec_command(cmd)
            assert s == '', 'exec: %s => %s' % (cmd, s)
            self.sentinel_resync(dashboard)
        else:
            raise NotImplementedError

    def setup_dashboards(self):
        log.info('Setting up dashboards')
        for dashboard in self.dashboards.values():
            if not dashboard.is_alive(self.zk):
                dashboard.stop_all()
                dashboard.run()
                assert dashboard.confirm_ping(retry=20), '%s:%s' % (dashboard.host, dashboard.port)

    def setup_sentinels(self):
        log.info('Setting up sentinels')
        for sentinel in self.sentinels.values():
            if not sentinel.is_alive(self.zk):
                sentinel.stop_all()
                sentinel.run()
                self.add_sentinel(sentinel.host, sentinel.port)

    def setup_servers(self):
        log.info('Setting up servers')
        if self.version == '2.0':
            self.ensure_slot_init()
        for server_group in self.server_groups.values():
            if not self.server_group_exists(server_group.group_id):
                self.add_server_group(server_group.group_id)
        for server_group in self.server_groups.values():
            for host_port in server_group.host_port_tuples:
                server = server_group.servers[host_port]
                if not server.is_alive(self.zk):
                    server.stop_all()
                    server.run()
                    self.add_server_to_group(server_group.group_id, server.host, server.port)
        offline_slot_ranges = self.get_offline_slot_ranges()
        for server_group in self.server_groups.values():
            from_slot, to_slot = server_group.get_slot_range(self.server_group_number)
            for offline_slot_range in offline_slot_ranges:
                from_offline, to_offline = offline_slot_range
                res = self._range_intersection(from_slot, to_slot, from_offline, to_offline)
                if res is not None:
                    self.create_slot_range(res[0], res[1], server_group.group_id)

    def setup_proxies(self):
        log.info('Setting up proxies')
        for proxy in self.proxies.values():
            if not proxy.is_alive(self.zk):
                proxy.stop_all()
                proxy.run()

    def setup(self):
        self.setup_dashboards()
        self.setup_sentinels()
        self.setup_servers()
        self.setup_proxies()

    def teardown_dashboards(self):
        log.info('Tearing down dashboards')
        for dashboard in self.dashboards.values():
            dashboard.stop_all()

    def teardown_sentinels(self):
        log.info('Tearing down sentinels')
        for sentinel in self.sentinels.values():
            sentinel.stop_all()

    def teardown_servers(self):
        log.info('Tearing down servers')
        for server_group in self.server_groups.values():
            for server in server_group.servers.values():
                server.stop_all()

    def teardown_proxies(self):
        for proxy in self.proxies.values():
            proxy.stop_all()

    def teardown(self):
        self.teardown_proxies()
        self.teardown_servers()
        self.teardown_sentinels()
        self.teardown_dashboards()
        self.remove_zk_root()

    def check_server_group_replication(self, group_id):
        log.info('Checking server_group_replication %d' % (group_id))
        if self.version in ['2.0', '3.1']:
            master_servers, slave_servers, other_servers = self.get_server_group(group_id)
            if len(master_servers) != 1:
                log.warn('Group %d, len(master_servers) = %d != 1' % (group_id, len(master_servers)))
                return False
            target_master_host, target_master_port = master_servers[0][:2]
            target_master_ip = CodisHelper.get_ip(target_master_host)
            if not CodisHelper.ping_server(host=master_servers[0][0], port=master_servers[0][1]):
                return False
            for host, port in slave_servers:
                t0, t1, t2 = CodisHelper.get_server_replication(host, port)
                master_host, master_port = t1
                master_ip = CodisHelper.get_ip(master_host)
                if master_ip != target_master_ip or master_port != target_master_port:
                    log.warn("Group %d, slave [%s|%s]:%d's master [%s|%s]:%d != group master [%s|%s]:%d" % (
                        group_id,
                        host, CodisHelper.get_ip(host), port,
                        master_host, master_ip, master_port,
                        target_master_host, target_master_ip, target_master_port,
                    ))
                    return False
            return True
        else:
            raise NotImplementedError

    def check_replication(self):
        for server_group in self.server_groups.values():
            assert self.check_server_group_replication(server_group.group_id), '%s' % (server_group._asdict())

    def check_server_proxy_connection(self):
        log.info('Checking server_proxy_connection')
        for server_group in self.server_groups.values():
            master_servers, slave_servers, other_servers = self.get_server_group(server_group.group_id)
            if len(master_servers) != 1:
                log.warn('Group %d, len(master_servers) = %d != 1' % (group_id, len(master_servers)))
                return False
            rand_key = None
            s_hn, s_pt = master_servers[0][:2]
            for proxy in self.proxies.values():
                p_hn = proxy.host
                p_pt = proxy.port
                connected = False
                for i in xrange(3):
                    if rand_key is None:  # redis instance is empty
                        rand_key = redis.StrictRedis(host=s_hn, port=s_pt).randomkey()
                    if redis.StrictRedis(host=p_hn, port=p_pt).exists(rand_key):
                        connected = True
                        break
                if rand_key is None:
                    log.info('server [%s:%s] rand_key is None, proxy [%s:%s]' % (s_hn, s_pt, p_hn, p_pt))
                    continue
                assert(connected)
                log.info('server [%s:%s] proxy [%s:%s] connected' % (s_hn, s_pt, p_hn, p_pt))

    def check_dashboard_ping(self):
        if self.version == '2.0':
            dashboard_host, dashboard_port = self.get_dashboard_host_port()

            def check_ping(data=None):
                res = requests.get(
                    'http://%s:%d/api/overview' % (dashboard_host, dashboard_port),
                )
                assert res.status_code == 200, '[%d][%s]' % (res.status_code, res.content)
                value = res.json()['last_ping']
                assert value != data, 'value = %s, data = %s' % (value, data)
                return value

            first_timestamp = retry_call(check_ping, fkwargs={'data': None}, exceptions=Exception, tries=20, delay=5, logger=log)
            second_timestamp = retry_call(check_ping, fkwargs={'data': first_timestamp}, exceptions=Exception, tries=20, delay=5, logger=log)
            log.info('[first_timestamp|second_timestamp] = [%s|%s]' % (first_timestamp, second_timestamp))
        else:
            raise NotImplementedError

    def check_all(self):
        self.check_server_proxy_connection()
        self.check_replication()
        self.check_dashboard_ping()

    def get_slot_group_dict(self, check_online=True):
        if self.version == '2.0':
            dashboard_host, dashboard_port = self.get_dashboard_host_port()
            url = 'http://%s:%d/api/slots' % (dashboard_host, dashboard_port)
            res = requests.get(url)
            data = json.loads(res.content)
            ret = dict()
            for d in data:
                if check_online:
                    assert d['state']['status'] == 'online', d
                ret[d['id']] = int(d['group_id'])
            assert len(ret) == 1024, '#slot_group_dict = %d != 1024' % (len(ret))
            return ret
        else:
            raise NotImplementedError

    @classmethod
    def _convert_group_slot_dict(cls, slot_group_dict):
        d = dict()
        for slot_id, group_id in slot_group_dict.iteritems():
            tmp = d.get(group_id, set())
            tmp.add(int(slot_id))
            d[group_id] = tmp
        return d

    def migrate_slot(self, from_slot, to_slot, new_group, delay=10):
        if self.version == '2.0':
            dashboard_host, dashboard_port = self.get_dashboard_host_port()
            log.info('Sending migration, slot [%d, %d] to group %d' % (from_slot, to_slot, new_group))
            res = requests.post(
                'http://%s:%d/api/migrate' % (dashboard_host, dashboard_port),
                json={
                    "from": from_slot,
                    "to": to_slot,
                    "new_group": new_group,
                    "delay": delay,
                },
            )
            assert res.status_code == 200, '[%d][%s]' % (res.status_code, res.content)
        else:
            raise NotImplementedError

    def manual_balance(self, target_group_count, delay=10):
        log.info('Manual balancing, target_group_count = %d' % (target_group_count))
        assert target_group_count > 0, 'target_group_count = %d' % (target_group_count)
        origin_slot_group_dict = self.get_slot_group_dict()
        origin_group_slot_dict = self._convert_group_slot_dict(origin_slot_group_dict)
        origin_group_count = len(origin_group_slot_dict)
        log.info('origin_group_count = %d' % (origin_group_count))
        target_slot_group_dict = dict()
        target_group_slot_dict = dict()
        for group_id in xrange(1, target_group_count + 1):
            from_slot = ServerGroup.from_slot(group_id, target_group_count)
            to_slot = ServerGroup.to_slot(group_id, target_group_count)
            target_group_slot_dict[group_id] = set(range(from_slot, to_slot + 1))
            for slot_id in xrange(from_slot, to_slot + 1):
                target_slot_group_dict[slot_id] = group_id
        max_group_count = max(target_group_count, origin_group_count)
        for group_id in xrange(target_group_count + 1, max_group_count + 1):
            target_group_slot_dict[group_id] = set()
        for group_id in xrange(origin_group_count + 1, max_group_count + 1):
            origin_group_slot_dict[group_id] = set()
        assert set(target_group_slot_dict.keys()) == set(origin_group_slot_dict.keys())
        tasks = list()
        for i in xrange(1028):
            assert i < 1026
            aim_group_id = None
            aim_group_count_to_fill = None
            for group_id in xrange(1, max_group_count + 1):
                group_count_to_fill = len(target_group_slot_dict[group_id]) - len(origin_group_slot_dict[group_id])
                diff_slots_count = len(target_group_slot_dict[group_id]) - len(target_group_slot_dict[group_id] & origin_group_slot_dict[group_id])
                if group_count_to_fill > 0 or diff_slots_count > 0:
                    if (aim_group_id is None) or (group_count_to_fill > aim_group_count_to_fill):
                        aim_group_id = group_id
                        aim_group_count_to_fill = group_count_to_fill
            if aim_group_id is None:
                break
            # print target_group_slot_dict[aim_group_id]
            # print origin_group_slot_dict[aim_group_id]
            slot_id = list(target_group_slot_dict[aim_group_id] - origin_group_slot_dict[aim_group_id])[0]
            origin_group_id = origin_slot_group_dict[slot_id]
            log.info('task appending: slot %d, group %d => group %d' % (slot_id, origin_group_id, aim_group_id))
            tasks.append((slot_id, aim_group_id))
            origin_group_slot_dict[origin_group_id].remove(slot_id)
            origin_group_slot_dict[aim_group_id].add(slot_id)
            origin_slot_group_dict[slot_id] = aim_group_id
        for slot, new_group in tasks:
            self.migrate_slot(slot, slot, new_group, delay)

    def get_migration_task_count(self):
        if self.version == '2.0':
            path = '/zk/codis/db_%s/migrate_tasks' % (self.product)
            res = self.zk.exists(path)
            return res.numChildren if res else 0
        else:
            raise NotImplementedError

    @classmethod
    def migrate_cluster_from_old_to_2(cls, old_cluster, new_cluster):
        assert old_cluster.version == '2.0', old_cluster._asdict()
        assert new_cluster.version == '2.0', new_cluster._asdict()
        assert len(old_cluster.server_groups) == len(new_cluster.server_groups)
        new_cluster.setup_dashboards()
        # new_cluster.setup_sentinels()
        new_cluster.setup_proxies()
        for group_id in xrange(1, len(new_cluster.server_groups) + 1):
            new_cluster.replace_servers_in_group(group_id)
            new_cluster.strip_server_group(group_id)
            new_cluster.setup_proxies()

    @classmethod
    def migrate_cluster_from_2_to_3_1(cls, old_cluster, new_cluster):
        assert old_cluster.version == '2.0', old_cluster._asdict()
        assert new_cluster.version == '3.1', new_cluster._asdict()
        assert len(old_cluster.server_groups) == len(new_cluster.server_groups)
        new_cluster.setup_dashboards()
        new_cluster.setup_sentinels()
        for group_id in xrange(1, len(new_cluster.server_groups) + 1):
            new_cluster.restore_servers_in_group(old_cluster, group_id)
        new_cluster.setup_proxies()
        for group_id in xrange(1, len(new_cluster.server_groups) + 1):
            new_cluster.replace_servers_in_group(group_id)
            new_cluster.strip_server_group(group_id)
            new_cluster.setup_proxies()
        old_cluster.teardown_proxies()
        old_cluster.teardown_servers()
        old_cluster.teardown_dashboards()

