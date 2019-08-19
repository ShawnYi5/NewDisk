# coding:utf-8
import os
import sys

# 将PRC接口模块加入加载模块路径
current_dir = os.path.split(os.path.realpath(__file__))[0]
sys.path.append(current_dir)
# ice_dir = os.path.join(current_dir, '..', 'AgentIce')
# sys.path.append(ice_dir)

try:
    import IMG

    ICE_IMG = IMG
except ImportError:
    ICE_IMG = None

try:
    import Utils

    ICE_UTILS = Utils
except ImportError:
    class FakeUtils(object):
        class SystemError(Exception):
            pass


    ICE_UTILS = FakeUtils

try:
    import BoxLogic

    ICE_LOGIC = BoxLogic
except ImportError:
    ICE_LOGIC = None

CDP_FILE_NO_CONTENT_ERR = 0x1A
