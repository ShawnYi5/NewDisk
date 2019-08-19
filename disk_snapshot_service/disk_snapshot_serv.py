import os

from cpkt.core import xlogging as lg

if 'DISABLE_LOGGING_CONF' in os.environ:
    print('DISABLE_LOGGING_CONF')
else:
    current_dir = os.path.split(os.path.realpath(__file__))[0]
    lg.set_logging_config(os.path.join(current_dir, 'logging.ini'))

if True:  # 必须在所有代码加载前配置日志参数，抑制IDE警告
    import sys
    from cpkt.core import xdebug
    from ice_service import service

_logger = lg.get_logger(__name__)

xdebug.XDebugHelper('/run/dss_dump_stack').start()

service.app = service.Server()
app_default_properties = [
    (r'Ice.ThreadPool.Server.Size', r'8'),
    (r'Ice.ThreadPool.Server.SizeMax', r'128'),
    (r'Ice.ThreadPool.Server.StackSize', r'8388608'),
    (r'Ice.ThreadPool.Client.Size', r'8'),
    (r'Ice.ThreadPool.Client.SizeMax', r'128'),
    (r'Ice.ThreadPool.Client.StackSize', r'8388608'),
    (r'Ice.Default.Host', r'localhost'),
    (r'Ice.Warn.Connections', r'1'),
    (r'Ice.ACM.Heartbeat', r'3'),  # HeartbeatAlways
    (r'Ice.ThreadPool.Client.ThreadIdleTime', r'900'),  # 15min
    (r'Ice.ThreadPool.Server.ThreadIdleTime', r'900'),  # 15min
    (r'Ice.MessageSizeMax', r'131072'),  # 单位KB, 128MB
    (r'ApiAdapter.Endpoints', r'tcp -h localhost -p 21119'),
    (r'ImgService4R.Proxy', r'img : tcp -h 127.0.0.1 -p 21101'),
    (r'ImgService4W.Proxy', r'img : tcp -h 127.0.0.1 -p 21104'),
    (r'CdpWriter.Proxy', r'img : tcp -h 127.0.0.1 -p 21130'),
]
service.app.main(sys.argv, '/etc/aio/disk_snapshot_serv.cfg', app_default_properties, _logger)
