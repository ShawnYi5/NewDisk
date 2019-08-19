import decimal
import os
import sys
import threading

import Ice

from basic_library import loadIce
from basic_library import xdata
from basic_library import xfunctions
from basic_library import xlogging

_logger = xlogging.getLogger(__name__)

_ice_service = None
_ice_service_locker = threading.Lock()


class IceService(object):
    """ICE服务封装类"""

    @staticmethod
    def get_ice_service():
        global _ice_service

        if _ice_service is None:
            with _ice_service_locker:
                if _ice_service is None:
                    _ice_service = IceService()
        return _ice_service

    def __init__(self):
        self.__imgPrx = None
        self.__logicPrx = None

        config = self.__generate_ice_config()
        self.__init_ice(config=config)

    @staticmethod
    def __generate_ice_config():
        init_data = Ice.InitializationData()
        init_data.properties = Ice.createProperties()
        init_data.properties.setProperty(r'Ice.ThreadPool.Client.Size', r'8')
        init_data.properties.setProperty(r'Ice.ThreadPool.Client.SizeMax', r'64')
        init_data.properties.setProperty(r'Ice.ThreadPool.Client.ThreadIdleTime', r'0')
        init_data.properties.setProperty(r'Ice.ThreadPool.Client.StackSize', r'8388608')
        init_data.properties.setProperty(r'ImageService.Proxy', r'img : tcp -h 127.0.0.1 -p 21101')
        init_data.properties.setProperty(r'BoxLogic.Proxy', r'logicInternal : tcp -h 127.0.0.1 -p 21109')
        init_data.properties.setProperty(r'Ice.Default.Host', r'127.0.0.1')
        init_data.properties.setProperty(r'Ice.Warn.Connections', r'1')
        init_data.properties.setProperty(r'Ice.LogFile', r'/var/log/clw_dss_ice.log')
        init_data.properties.setProperty(r'Ice.RetryIntervals', r'0')

        init_data.properties.setProperty(r'Ice.MessageSizeMax', r'65536')  # 64MB

        init_data.properties.setProperty(r'Ice.ACM.Heartbeat', r'3')  # BoxService KernelTcp 会检测心跳

        config_path = r'/etc/aio/dss.cfg'
        if os.path.exists(config_path):
            init_data.properties.load(config_path)

        return init_data

    def __init_ice(self, config):
        self.communicator = Ice.initialize(sys.argv, config)

    def get_img_prx(self):
        if self.__imgPrx is None:
            self.__imgPrx = loadIce.ICE_IMG.ImgServicePrx.checkedCast(
                self.communicator.propertyToProxy(r'ImageService.Proxy')
            )
        return self.__imgPrx

    def get_logic_prx(self):
        if self.__logicPrx is None:
            self.__logicPrx = loadIce.ICE_LOGIC.LogicInternalPrx.checkedCast(
                self.communicator.propertyToProxy(r'BoxLogic.Proxy')
            )
        return self.__logicPrx


_image_service = None
_image_service_locker = threading.Lock()


class ImageService(object):
    """ImageService 相关操作封装"""

    @staticmethod
    def get_image_service():
        global _image_service

        if _image_service is None:
            with _image_service_locker:
                if _image_service is None:
                    _image_service = ImageService(IceService.get_ice_service())
        return _image_service

    def __init__(self, ice_service):
        self._ice_service = ice_service
        xfunctions.TraceDecorator(['get_image_service', ], self, _logger).decorate()
        xfunctions.ExceptionHandlerDecorator({
            'delete_snapshot_in_qcow_file': {
                'msg': '删除快照镜像',
                'code': xdata.ERROR_DELETE_DISK_SNAPSHOT_FAILED,
            },
        }, self, _logger).decorate()

    def delete_snapshot_in_qcow_file(self, file_path, snapshot_name):
        returned = self._ice_service.get_img_prx().DelSnaport(
            loadIce.ICE_IMG.ImageSnapshotIdent(file_path, snapshot_name)
        )
        if returned == -2:
            xlogging.raise_and_logging_error(
                r'快照镜像({})正在使用中，无法回收'.format(snapshot_name),
                r'delete snapshot {} - {} failed, using'.format(file_path, snapshot_name),
                print_args=False)
        elif returned != 0:
            xlogging.raise_and_logging_error(
                r'删除快照镜像({})失败'.format(snapshot_name),
                r'delete snapshot {} - {} failed, {}'.format(file_path, snapshot_name, returned),
                print_args=False)


_logic_service = None
_logic_service_locker = threading.Lock()


class LogicService(object):
    """LogicService 相关操作封装"""

    @staticmethod
    def get_logic_service():
        global _logic_service

        if _logic_service is None:
            with _logic_service_locker:
                if _logic_service is None:
                    _logic_service = LogicService(IceService.get_ice_service())
        return _logic_service

    def __init__(self, ice_service):
        self._ice_service = ice_service
        xfunctions.TraceDecorator(['get_logic_service', ], self, _logger).decorate()
        xfunctions.ExceptionHandlerDecorator({
            'query_cdp_file_timestamp_range': {
                'msg': '读取CDP数据',
                'code': xdata.ERROR_QUERY_CDP_FILE_TIMESTAMP_RANGE_FAILED,
            },
        }, self, _logger).decorate()

    def query_cdp_file_timestamp_range(self, path: str, discard_dirty_data: bool = False) -> tuple:
        """查询CDP文件中的时间范围

        :remark:
            对于封闭的CDP文件使用时，需要丢弃脏数据区域；但是对于还在写入数据中的CDP文件，最新的数据大概率是脏数据
        :param path:
            cdp文件路径
        :param discard_dirty_data:
            是否丢弃脏数据
        :return: (decimal, decimal)
            如果该CDP文件中没有有效的数据信息，那么会返回 None, None
        """
        try:
            result = self._ice_service.get_logic_prx().queryCdpTimestampRange(path, discard_dirty_data)
        except loadIce.ICE_UTILS.SystemError as se:
            if se.rawCode == loadIce.CDP_FILE_NO_CONTENT_ERR:
                return None, None
            raise se

        result_list = result.split()
        assert len(result_list) == 2, f'query {path} timestamp range with error. {discard_dirty_data} {result}'
        return (xfunctions.convert_timestamp_float_to_decimal(float(result_list[0])),
                xfunctions.convert_timestamp_float_to_decimal(float(result_list[1])))

    def query_cdp_file_timestamp(
            self, path: str, timestamp: decimal.Decimal, mode: str = 'forwards') -> decimal.Decimal:
        """查询CDP文件中的确实存在的时间戳

        :remark:
            该接口需配合query_cdp_file_timestamp_range使用，不可查询query_cdp_file_timestamp_range返回结果以外的数据
        :param path:
            cdp文件路径
        :param timestamp:
            需要查询的预期时间戳。该时间戳逻辑上存在，但实际上可能不存在，需要修正
        :param mode:
            'forwards', 'backwards'
        :return:
        """
        result = self._ice_service.get_logic_prx().queryCdpTimestamp(path, f'{timestamp}|{mode}')
        result_list = result.split()
        assert len(result_list) == 1, (f'query {path} '
                                       f'timestamp {timestamp}({xfunctions.humanize_timestamp(timestamp)}) error')
        return xfunctions.convert_timestamp_float_to_decimal(float(result_list[0]))

    def format_cdp_file_timestamp(self, timestamp: decimal.Decimal) -> str:
        return self._ice_service.get_logic_prx().formatCdpTimestamp(f'{timestamp}')
