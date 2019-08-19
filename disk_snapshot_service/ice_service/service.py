from cpkt.core import exc
from cpkt.core import xlogging as lg
from cpkt.icehelper import application
from cpkt.rpc import ice

import interface_data_define as idd
from basic_library import xfunctions as xf
from service_logic import consume_journal
from service_logic import generate_journal
from service_logic import handle_operation

_logger = lg.get_logger(__name__)


class SnapshotI(ice.SnapshotApi.Snapshot):
    EXECUTE = {
        # 生成创建快照存储的日志
        "generate_journal_for_create": (
            idd.GenerateJournalForCreateParamsSchema,
            generate_journal.for_create,
            idd.EmptySchema,
        ),
        # 生成销毁快照存储的日志
        "generate_journal_for_destroy": (
            idd.GenerateJournalForDestroyParamsSchema,
            generate_journal.for_destroy,
            idd.EmptySchema,
        ),
        # 创建快照存储，获得写权限原生句柄
        "create_snapshot": (
            idd.CreateSnapshotParamsSchema,
            consume_journal.create_snapshot,
            idd.CreateSnapshotResultSchema,
        ),
        # 关闭快照存储原生句柄，并释放锁定的快照存储
        "close_snapshot": (
            idd.CloseSnapshotParamsSchema,
            handle_operation.close_snapshot,
            idd.EmptySchema,
        ),
        # 打开快照存储，可选获得读取原生句柄
        # remark:
        #    支持延迟打开原生读句柄，参考 get_raw_handle
        "open_snapshot": (
            idd.OpenSnapshotParamsSchema,
            handle_operation.open_snapshot,
            idd.OpenSnapshotResultSchema,
        ),
        # 获取原生句柄
        # remark:
        #    支持延迟打开原生读句柄，如果 open_snapshot 未打开原生读句柄，那么可以在 open_snapshot 后，调用此接口获取
        "get_raw_handle": (
            idd.GetRawHandleParamsSchema,
            handle_operation.get_raw_handle,
            idd.GetRawHandleResultSchema,
        ),
        # 设置写句柄关闭时的工作模式
        # remark:
        #   支持 直接使用hash文件、修正后使用hash文件
        "set_hash_mode": (
            idd.SetHashModeParamsSchema,
            handle_operation.set_hash_mode,
            idd.EmptySchema,
        ),
    }

    def OpWithBinary(self, call, in_json, in_raw, current=None):
        _ = self
        _ = current
        _logger.info(f'OpWithBinary {call} : {in_json} || {lg.to_unicode(in_raw)}')
        out_json = '{"msg": "I am in OpWithBinary"}'
        out_raw = bytes()
        return out_json, out_raw

    def Op(self, call, in_json, current=None):
        _ = self
        _ = current
        op_index = xf.generate_unique_number(xf.UNIQUE_ICE_OP_INDEX)
        _logger.info(f'Op [{op_index}] {call} : {in_json}')
        try:
            params, errors = SnapshotI.EXECUTE[call][0]().loads(in_json)
            assert not errors, ('内部异常，代码 LoadJsonFailed', f'load failed {errors}', 0,)

            result = SnapshotI.EXECUTE[call][1](params)

            out_json, errors = SnapshotI.EXECUTE[call][2]().dumps(result, ensure_ascii=False)
            assert not errors, ('内部异常，代码 DumpJsonFailed', f'dump failed {errors}', 0,)

            _logger.info(f'Op [{op_index}] {call} : {out_json}')
            return out_json
        except Exception as e:
            _logger.error(f'Op [{op_index}] {call} failed\n{lg.format_exception(e)}')
            raise exc.standardize_exception(e)


class Server(application.Application):
    def run(self, args):
        adapter = self.communicator().createObjectAdapter("ApiAdapter")
        adapter.add(SnapshotI(), self.communicator().stringToIdentity("dss"))
        adapter.activate()
        self.communicator().waitForShutdown()
        return 0


app = None  # type: Server

__ReadImgPrx = None
__WriteImgPrx = None
__CdpPrx = None


def get_read_img_prx():
    global __ReadImgPrx
    if not __ReadImgPrx:
        __ReadImgPrx = ice.IMG.ImgServicePrx.checkedCast(app.communicator().propertyToProxy(r'ImgService4R.Proxy'))
    return __ReadImgPrx


def get_write_img_prx():
    global __WriteImgPrx
    if not __WriteImgPrx:
        __WriteImgPrx = ice.IMG.ImgServicePrx.checkedCast(app.communicator().propertyToProxy(r'ImgService4W.Proxy'))
    return __WriteImgPrx


def get_cdp_prx():
    global __CdpPrx
    if not __CdpPrx:
        __CdpPrx = ice.IMG.ImgServicePrx.checkedCast(app.communicator().propertyToProxy(r'CdpWriter.Proxy'))
    return __CdpPrx


def convert_proxy_to_string(prx):
    return app.communicator().proxyToString(prx)


def convert_string_to_prx(s):
    return ice.IMG.ImgServicePrx.checkedCast(app.communicator().stringToProxy(s))
