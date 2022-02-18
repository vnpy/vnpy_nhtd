import sys
import pytz
from datetime import datetime
from time import sleep
from typing import Dict, List, Tuple, Any, Set
from copy import copy
from vnpy.event.engine import EventEngine
from pathlib import Path

from vnpy.trader.constant import (
    Direction,
    Offset,
    Exchange,
    OrderType,
    Product,
    Status,
    OptionType
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
)
from vnpy.trader.utility import get_folder_path
from vnpy.trader.event import EVENT_TIMER
from vnpy.event import Event

from ..api import MdApi, FuturesTdApi, StockTdApi

from .futures_constant import (
    THOST_FTDC_OAS_Submitted,
    THOST_FTDC_OAS_Accepted,
    THOST_FTDC_OAS_Rejected,
    THOST_FTDC_OST_NoTradeQueueing,
    THOST_FTDC_OST_PartTradedQueueing,
    THOST_FTDC_OST_AllTraded,
    THOST_FTDC_OST_Canceled,
    THOST_FTDC_D_Buy,
    THOST_FTDC_D_Sell,
    THOST_FTDC_PD_Long,
    THOST_FTDC_PD_Short,
    THOST_FTDC_OPT_LimitPrice,
    THOST_FTDC_OPT_AnyPrice,
    THOST_FTDC_OF_Open,
    THOST_FTDC_OFEN_Close,
    THOST_FTDC_OFEN_CloseYesterday,
    THOST_FTDC_OFEN_CloseToday,
    THOST_FTDC_PC_Futures,
    THOST_FTDC_PC_Options,
    THOST_FTDC_PC_SpotOption,
    THOST_FTDC_PC_Combination,
    THOST_FTDC_CP_CallOptions,
    THOST_FTDC_CP_PutOptions,
    THOST_FTDC_HF_Speculation,
    THOST_FTDC_CC_Immediately,
    THOST_FTDC_FCC_NotForceClose,
    THOST_FTDC_TC_GFD,
    THOST_FTDC_VC_AV,
    THOST_FTDC_TC_IOC,
    THOST_FTDC_VC_CV,
    THOST_FTDC_AF_Delete
)
from .stock_constant import (
    SZSE_FTDC_CallOrPut_A,
    SZSE_FTDC_CallOrPut_E,
    SZSE_FTDC_OrdType_Limit,
    SZSE_FTDC_OrdType_Market,
    SZSE_FTDC_OC_Open,
    SZSE_FTDC_OC_Close,
    SZSE_FTDC_SIDE_Buy,
    SZSE_FTDC_SIDE_Sell,
    SZSE_FTDC_Status_Success,
    SZSE_FTDC_Status_Trade,
    SZSE_FTDC_Status_All,
    SZSE_FTDC_Status_Cancel,
    SZSE_FTDC_Status_Reject,
    SZSE_FTDC_TimeInForce_FOK,
    SZSE_FTDC_TimeInForce_IOC,
    SZSE_FTDC_TimeInForce_GFD,
)
from .stock_error import ERROR_MSG


# 期货委托状态映射
STATUS_FUTURES2VT: Dict[str, Status] = {
    THOST_FTDC_OAS_Submitted: Status.SUBMITTING,
    THOST_FTDC_OAS_Accepted: Status.SUBMITTING,
    THOST_FTDC_OAS_Rejected: Status.REJECTED,
    THOST_FTDC_OST_NoTradeQueueing: Status.NOTTRADED,
    THOST_FTDC_OST_PartTradedQueueing: Status.PARTTRADED,
    THOST_FTDC_OST_AllTraded: Status.ALLTRADED,
    THOST_FTDC_OST_Canceled: Status.CANCELLED
}

# 期货多空方向映射
DIRECTION_VT2FUTURES: Dict[Direction, str] = {
    Direction.LONG: THOST_FTDC_D_Buy,
    Direction.SHORT: THOST_FTDC_D_Sell
}
DIRECTION_FUTURES2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2FUTURES.items()}
DIRECTION_FUTURES2VT[THOST_FTDC_PD_Long] = Direction.LONG
DIRECTION_FUTURES2VT[THOST_FTDC_PD_Short] = Direction.SHORT

# 期货委托类型映射
ORDERTYPE_VT2FUTURES: Dict[OrderType, str] = {
    OrderType.LIMIT: THOST_FTDC_OPT_LimitPrice,
    OrderType.MARKET: THOST_FTDC_OPT_AnyPrice
}
ORDERTYPE_FUTURES2VT: Dict[str, OrderType] = {v: k for k, v in ORDERTYPE_VT2FUTURES.items()}

# 期货开平方向映射
OFFSET_VT2FUTURES: Dict[Offset, str] = {
    Offset.OPEN: THOST_FTDC_OF_Open,
    Offset.CLOSE: THOST_FTDC_OFEN_Close,
    Offset.CLOSETODAY: THOST_FTDC_OFEN_CloseToday,
    Offset.CLOSEYESTERDAY: THOST_FTDC_OFEN_CloseYesterday,
}
OFFSET_FUTURES2VT: Dict[str, Offset] = {v: k for k, v in OFFSET_VT2FUTURES.items()}

# 期货交易所映射
EXCHANGE_FUTURES2VT: Dict[str, Exchange] = {
    "CFFEX": Exchange.CFFEX,
    "SHFE": Exchange.SHFE,
    "CZCE": Exchange.CZCE,
    "DCE": Exchange.DCE,
    "INE": Exchange.INE
}

# 期货产品类型映射
PRODUCT_FUTURES2VT: Dict[str, Product] = {
    THOST_FTDC_PC_Futures: Product.FUTURES,
    THOST_FTDC_PC_Options: Product.OPTION,
    THOST_FTDC_PC_SpotOption: Product.OPTION,
    THOST_FTDC_PC_Combination: Product.SPREAD
}

# 期货期权类型映射
OPTIONTYPE_FUTURES2VT: Dict[str, OptionType] = {
    THOST_FTDC_CP_CallOptions: OptionType.CALL,
    THOST_FTDC_CP_PutOptions: OptionType.PUT
}

# 其他常量
MAX_FLOAT = sys.float_info.max                  # 浮点数极限值
CHINA_TZ = pytz.timezone("Asia/Shanghai")       # 中国时区
EVENT_NH_EXERCISE = "eNhExercise"
EVENT_NH_EXERCISE_LOG = "eNhExerciseLog"

# 股票委托状态映射
STATUS_STOCK2VT: Dict[str, Status] = {
    SZSE_FTDC_Status_Success: Status.NOTTRADED,
    SZSE_FTDC_Status_Trade: Status.PARTTRADED,
    SZSE_FTDC_Status_All: Status.ALLTRADED,
    SZSE_FTDC_Status_Cancel: Status.CANCELLED,
    SZSE_FTDC_Status_Reject: Status.REJECTED
}

# 股票多空方向映射
DIRECTION_STOCK2VT: Dict[str, Direction] = {
    SZSE_FTDC_SIDE_Buy: Direction.LONG,
    SZSE_FTDC_SIDE_Sell: Direction.SHORT
}
DIRECTION_VT2STOCK = {v: k for k, v in DIRECTION_STOCK2VT.items()}

# 股票委托类型映射
ORDERTYPE_VT2STOCK: Dict[OrderType, Tuple] = {
    OrderType.MARKET: (SZSE_FTDC_OrdType_Market, SZSE_FTDC_TimeInForce_GFD),
    OrderType.LIMIT: (SZSE_FTDC_OrdType_Limit, SZSE_FTDC_TimeInForce_GFD),
    OrderType.FAK: (SZSE_FTDC_OrdType_Limit, SZSE_FTDC_TimeInForce_IOC),
    OrderType.FAK: (SZSE_FTDC_OrdType_Limit, SZSE_FTDC_TimeInForce_FOK),
}
ORDERTYPE_STOCK2VT: Dict[Tuple, OrderType] = {v: k for k, v in ORDERTYPE_VT2STOCK.items()}

# 股票开平方向映射
OFFSET_STOCK2VT: Dict[str, Offset] = {
    SZSE_FTDC_OC_Open: Offset.OPEN,
    SZSE_FTDC_OC_Close: Offset.CLOSE
}
OFFSET_VT2STOCK: Dict[Offset, str] = {v: k for k, v in OFFSET_STOCK2VT.items()}

# 行情交易所映射
EXCHANGE_MD2VT: Dict[str, Exchange] = {
    "CFFEX": Exchange.CFFEX,
    "SHFE": Exchange.SHFE,
    "CZCE": Exchange.CZCE,
    "DCE": Exchange.DCE,
    "INE": Exchange.INE,
}
EXCHANGE_VT2MD: Dict[Exchange, str] = {
    Exchange.CFFEX: "CFFEX",
    Exchange.SHFE: "SHFE",
    Exchange.CZCE: "CZCE",
    Exchange.DCE: "DCE",
    Exchange.SSE: "SSE",
    Exchange.SZSE: "SZSE"
}

# 期权类型映射
OPTIONTYPE_STOCK2VT: Dict[str, OptionType] = {
    SZSE_FTDC_CallOrPut_E: OptionType.CALL,
    SZSE_FTDC_CallOrPut_A: OptionType.PUT
}

# 合约数据全局缓存字典
symbol_contract_map: Dict[str, ContractData] = {}


class NhGateway(BaseGateway):
    """
    vn.py用于对接南华期货的交易接口。
    """

    default_name: str = "NHTD"

    default_setting: Dict[str, str] = {
        "用户名": "",
        "密码": "",
        "股东号": "",
        "交易服务器": "",
        "行情服务器": "",
        "产品代码": "",
        "授权码": "",
        "开发者编码": "",
        "开发者授权": "",
        "行情服务器登录用户": "",
        "行情服务器登录密码": ""
    }

    def __init__(self, event_engine: EventEngine, td_class: Any, gateway_name: str) -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.md_api: "NhMdApi" = NhMdApi(self)
        self.td_api = td_class(self)

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        userid: str = setting["用户名"]
        password: str = setting["密码"]
        party_id: str = setting["股东号"]
        td_address: str = setting["交易服务器"]
        md_address: str = setting["行情服务器"]
        appid: str = setting["产品代码"]
        auth_code: str = setting["授权码"]
        code: str = setting["开发者编码"]
        license: str = setting["开发者授权"]
        md_userid: str = setting["行情服务器登录用户"]
        md_password: str = setting["行情服务器登录密码"]

        if not td_address.startswith("tcp://"):
            td_address = "tcp://" + td_address

        if not md_address.startswith("tcp://"):
            md_address = "tcp://" + md_address

        self.td_api.connect(td_address, userid, password, party_id, appid, auth_code)
        self.md_api.connect(md_address, md_userid, md_password, code, license)

        self.init_query()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.md_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        vt_orderid = self.td_api.send_order(req)
        return vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.td_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        self.td_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        self.td_api.query_position()

    def close(self) -> None:
        """关闭接口"""
        self.td_api.close()
        self.md_api.close()

    def write_error(self, msg: str, error: dict) -> None:
        """输出错误信息日志"""
        error_id: int = error["ErrorID"]
        error_msg: str = error["ErrorMsg"]
        if not error_msg:
            error_msg: str = ERROR_MSG.get(error_id, "")

        msg: str = f"{msg}，代码：{error_id}，信息：{error_msg}"
        self.write_log(msg)

    def process_timer_event(self, event) -> None:
        """定时事件处理"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0

        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

        self.md_api.update_date()

    def init_query(self) -> None:
        """初始化查询任务"""
        self.count: int = 0
        self.query_functions: list = [self.query_account, self.query_position]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def send_exercise(self, req: dict) -> None:
        """委托行权"""
        self.td_api.send_exercise(req)

    def cancel_exercise(self, req: CancelRequest) -> None:
        """撤销行权"""
        self.td_api.cancel_exercise(req)

    def query_exercise(self) -> None:
        """查询行权记录"""
        self.td_api.query_exercise()


class NhFuturesGateway(NhGateway):

    default_name: str = "NHFUTURES"

    exchanges: List[str] = list(EXCHANGE_FUTURES2VT.values())

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """构造函数"""
        super().__init__(event_engine, NhFuturesTdApi, gateway_name)


class NhStockGateway(NhGateway):

    default_name: str = "NHSTOCK"

    exchanges: List[str] = [Exchange.SSE, Exchange.SZSE]

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """构造函数"""
        super().__init__(event_engine, NhStockTdApi, gateway_name)


class NhMdApi(MdApi):
    """"""

    def __init__(self, gateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.reqid: int = 0

        self.connect_status: bool = False
        self.login_status: bool = False
        self.subscribed: Set = set()

        self.userid: str = ""
        self.password: str = ""
        self.code: str = "xuwanxin"
        self.license: str = "xwx123"

        self.current_date: str = datetime.now().strftime("%Y%m%d")

    def connect(self, address: str, userid: str, password: str, code: str, license: str) -> None:
        """连接服务器"""
        self.userid = userid
        self.password = password
        self.code = code
        self.license = license

        # 如果没有连接，就先发起连接
        if not self.connect_status:
            self.createMdApi()
            self.registerFront(address)
            self.init()

            self.connect_status = True

        # 如果已经连接就即刻登录
        elif not self.login_status:
            self.login()

    def login(self) -> None:
        """用户登录"""
        req: dict = {
            "developer_code": self.code,
            "developer_license": self.license,
            "user_id": self.userid,
            "user_password": self.password,
        }
        self.reqid += 1
        self.reqUtpLogin(req, self.reqid)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        exchange_str: str = EXCHANGE_VT2MD[req.exchange]

        if req.exchange in {Exchange.SSE, Exchange.SZSE}:
            # 指数: MD001
            # 股票: MD002
            # 债券: MD003
            # ETF: MD004
            # 期权: M0301
            key: str = f"{exchange_str}.M0301.{req.symbol}"
        else:
            key: str = f"{exchange_str}.{req.symbol}"

        if self.login_status:
            self.reqid += 1
            self.reqSubscribe(key, self.reqid)

        self.subscribed.add(key)

    def close(self) -> None:
        """关闭连接"""
        if self.connect_status:
            self.exit()

    def update_date(self) -> None:
        """更新当前日期"""
        self.current_date = datetime.now().strftime("%Y%m%d")

    def onFrontConnected(self) -> None:
        """服务器连接成功回报"""
        self.gateway.write_log("行情服务器连接成功")
        self.login()

    def onFrontDisConnected(self) -> None:
        """服务器连接断开回报"""
        self.login_status = False
        self.gateway.write_log("行情服务器连接断开")

    def onRspError(self, error: dict, reqid: int) -> None:
        """请求报错回报"""
        msg: str = f"行情接口报错，错误信息{error['error_message']}"
        self.gateway.write_log(msg)

    def onRtnMarketData(self, data: dict) -> None:
        """订阅行情回报"""
        symbol: str = data["instrument_id"]

        contract: ContractData = symbol_contract_map.get(symbol, None)
        if not contract:
            return

        timestamp: str = f"{self.current_date} {data['update_time']}.{int(data['update_millisec']/100)}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S.%f")
        dt: datetime = CHINA_TZ.localize(dt)

        tick: TickData = TickData(
            symbol=symbol,
            exchange=contract.exchange,
            datetime=dt,
            name=contract.name,
            volume=data["volume"],
            open_interest=data["open_interest"],
            last_price=data["last_price"],
            limit_up=data["upper_limit_price"],
            limit_down=data["lower_limit_price"],
            open_price=adjust_price(data["open_price"]),
            high_price=adjust_price(data["highest_price"]),
            low_price=adjust_price(data["lowest_price"]),
            pre_close=adjust_price(data["pre_close_price"]),
            bid_price_1=adjust_price(data["bid_price1"]),
            ask_price_1=adjust_price(data["ask_price1"]),
            bid_volume_1=data["bid_volume1"],
            ask_volume_1=data["ask_volume1"],
            gateway_name=self.gateway_name
        )

        if data["bid_volume2"] or data["ask_volume2"]:
            tick.bid_price_2 = adjust_price(data["bid_price2"])
            tick.bid_price_3 = adjust_price(data["bid_price3"])
            tick.bid_price_4 = adjust_price(data["bid_price4"])
            tick.bid_price_5 = adjust_price(data["bid_price5"])

            tick.ask_price_2 = adjust_price(data["ask_price2"])
            tick.ask_price_3 = adjust_price(data["ask_price3"])
            tick.ask_price_4 = adjust_price(data["ask_price4"])
            tick.ask_price_5 = adjust_price(data["ask_price5"])

            tick.bid_volume_2 = data["bid_volume2"]
            tick.bid_volume_3 = data["bid_volume3"]
            tick.bid_volume_4 = data["bid_volume4"]
            tick.bid_volume_5 = data["bid_volume5"]

            tick.ask_volume_2 = data["ask_volume2"]
            tick.ask_volume_3 = data["ask_volume3"]
            tick.ask_volume_4 = data["ask_volume4"]
            tick.ask_volume_5 = data["ask_volume5"]

        self.gateway.on_tick(tick)

    def onRspUtpLogin(self, data: dict, reqid: int) -> None:
        """用户登录请求回报"""
        if not data["response_code"]:
            self.login_status = True
            self.gateway.write_log("行情服务器登录成功")

            for key in self.subscribed:
                self.reqid += 1
                self.reqSubscribe(key, self.reqid)
        else:
            msg: str = f"行情服务器登录失败，错误信息{data['response_string']}"
            self.gateway.write_log(msg)

    def onRspSubscribe(self, data: dict, reqid: int) -> None:
        """订阅行情回报"""
        if data["response_code"]:
            msg: str = f"行情订阅失败，错误信息{data['response_string']}"
            self.gateway.write_log(msg)


class NhFuturesTdApi(FuturesTdApi):
    """"""

    def __init__(self, gateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.reqid: int = 0
        self.order_ref: int = 0

        self.connect_status: bool = False
        self.login_status: bool = False
        self.auth_status: bool = False
        self.login_failed: bool = False
        self.contract_inited: bool = False

        self.userid: str = ""
        self.password: str = ""
        self.auth_code: str = ""
        self.appid: str = ""
        self.product_info: str = ""

        self.frontid: int = 0
        self.sessionid: int = 0

        self.order_data: List[dict] = []
        self.trade_data: List[dict] = []
        self.positions: Dict[str, PositionData] = {}
        self.sysid_orderid_map: Dict[str, str] = {}

    def onFrontConnected(self) -> None:
        """服务器连接成功回报"""
        self.gateway.write_log("交易服务器连接成功")

        if self.auth_code:
            self.authenticate()
        else:
            self.login()

    def onFrontDisconnected(self, reason: int) -> None:
        """服务器连接断开回报"""
        self.login_status = False
        self.gateway.write_log(f"交易服务器连接断开，原因{reason}")

    def onRspAuthenticate(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """用户授权验证回报"""
        if not error['ErrorID']:
            self.auth_status = True
            self.gateway.write_log("交易服务器授权验证成功")
            self.login()
        else:
            self.gateway.write_error("交易服务器授权验证失败", error)

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """用户登录请求回报"""
        if not error["ErrorID"]:
            self.frontid = data["FrontID"]
            self.sessionid = data["SessionID"]
            self.login_status = True
            self.gateway.write_log("交易服务器登录成功")

            # 自动确认结算单
            req: dict = {
                "InvestorID": self.userid
            }
            self.reqid += 1
            self.reqSettlementInfoConfirm(req, self.reqid)
        else:
            self.login_failed = True

            self.gateway.write_error("交易服务器登录失败", error)

    def onRspOrderInsert(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """委托下单失败回报"""
        order_ref: str = data["OrderRef"]
        orderid: str = f"{self.frontid}_{self.sessionid}_{order_ref}"

        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map[symbol]

        order: OrderData = OrderData(
            symbol=symbol,
            exchange=contract.exchange,
            orderid=orderid,
            direction=DIRECTION_FUTURES2VT[data["Direction"]],
            offset=OFFSET_FUTURES2VT.get(data["CombOffsetFlag"], Offset.NONE),
            price=data["LimitPrice"],
            volume=data["VolumeTotalOriginal"],
            status=Status.REJECTED,
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)

        self.gateway.write_error("交易委托失败", error)

    def onRspOrderAction(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """委托撤单失败回报"""
        self.gateway.write_error("交易撤单失败", error)

    def onRspSettlementInfoConfirm(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """确认结算单回报"""
        self.gateway.write_log("结算信息确认成功")

        while True:
            self.reqid += 1
            n: int = self.reqQryInstrument({}, self.reqid)

            if not n:
                break
            else:
                sleep(1)

    def onRspQryInvestorPosition(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """持仓查询回报"""
        if not data:
            return

        # 必须已经收到了合约信息后才能处理
        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map.get(symbol, None)

        if contract:
            # 获取之前缓存的持仓数据缓存
            key: str = f"{data['InstrumentID'], data['PosiDirection']}"
            position: PositionData = self.positions.get(key, None)
            if not position:
                position: PositionData = PositionData(
                    symbol=data["InstrumentID"],
                    exchange=contract.exchange,
                    direction=DIRECTION_FUTURES2VT[data["PosiDirection"]],
                    gateway_name=self.gateway_name
                )
                self.positions[key] = position

            # 对于上期所昨仓需要特殊处理
            if position.exchange in [Exchange.SHFE, Exchange.INE]:
                if data["YdPosition"] and not data["TodayPosition"]:
                    position.yd_volume = data["Position"]
            # 对于其他交易所昨仓的计算
            else:
                position.yd_volume = data["Position"] - data["TodayPosition"]

            # 获取合约的乘数信息
            size: int = contract.size

            # 计算之前已有仓位的持仓总成本
            cost: float = position.price * position.volume * size

            # 累加更新持仓数量和盈亏
            position.volume += data["Position"]
            position.pnl += data["PositionProfit"]

            # 计算更新后的持仓总成本和均价
            if position.volume and size:
                cost += data["PositionCost"]
                position.price = cost / (position.volume * size)

            # 更新仓位冻结数量
            if position.direction == Direction.LONG:
                position.frozen += data["ShortFrozen"]
            else:
                position.frozen += data["LongFrozen"]

        if last:
            for position in self.positions.values():
                self.gateway.on_position(position)

            self.positions.clear()

    def onRspQryTradingAccount(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """资金查询回报"""
        if "AccountID" not in data:
            return

        account: AccountData = AccountData(
            accountid=data["AccountID"],
            balance=data["Balance"],
            frozen=data["FrozenMargin"] + data["FrozenCash"] + data["FrozenCommission"],
            gateway_name=self.gateway_name
        )
        account.available = data["Available"]
        self.gateway.on_account(account)

    def onRspQryInstrument(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """合约查询回报"""
        product: Product = PRODUCT_FUTURES2VT.get(data["ProductClass"], None)
        if product:
            contract: ContractData = ContractData(
                symbol=data["InstrumentID"],
                exchange=EXCHANGE_FUTURES2VT[data["ExchangeID"]],
                name=data["InstrumentName"],
                product=product,
                size=data["VolumeMultiple"],
                pricetick=data["PriceTick"],
                gateway_name=self.gateway_name
            )

            # 期权相关
            if contract.product == Product.OPTION:
                # 移除郑商所期权产品名称带有的C/P后缀
                if contract.exchange == Exchange.CZCE:
                    contract.option_portfolio = data["ProductID"][:-1]
                else:
                    contract.option_portfolio = data["ProductID"]

                contract.option_underlying = data["UnderlyingInstrID"]
                contract.option_type = OPTIONTYPE_FUTURES2VT.get(data["OptionsType"], None)
                contract.option_strike = data["StrikePrice"]
                contract.option_index = str(data["StrikePrice"])
                contract.option_expiry = datetime.strptime(data["ExpireDate"], "%Y%m%d")

            self.gateway.on_contract(contract)

            symbol_contract_map[contract.symbol] = contract

        if last:
            self.contract_inited = True
            self.gateway.write_log("合约信息查询成功")

            for data in self.order_data:
                self.onRtnOrder(data)
            self.order_data.clear()

            for data in self.trade_data:
                self.onRtnTrade(data)
            self.trade_data.clear()

    def onRtnOrder(self, data: dict) -> None:
        """委托更新推送"""
        if not self.contract_inited:
            self.order_data.append(data)
            return

        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map[symbol]

        frontid: int = data["FrontID"]
        sessionid: int = data["SessionID"]
        order_ref: str = data["OrderRef"]
        orderid: str = f"{frontid}_{sessionid}_{order_ref}"

        timestamp: str = f"{data['InsertDate']} {data['InsertTime']}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S")
        dt: datetime = CHINA_TZ.localize(dt)

        order: OrderData = OrderData(
            symbol=symbol,
            exchange=contract.exchange,
            orderid=orderid,
            type=ORDERTYPE_FUTURES2VT[data["OrderPriceType"]],
            direction=DIRECTION_FUTURES2VT[data["Direction"]],
            offset=OFFSET_FUTURES2VT[data["CombOffsetFlag"]],
            price=data["LimitPrice"],
            volume=data["VolumeTotalOriginal"],
            traded=data["VolumeTraded"],
            status=STATUS_FUTURES2VT[data["OrderStatus"]],
            datetime=dt,
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)

        self.sysid_orderid_map[data["OrderSysID"]] = orderid

    def onRtnTrade(self, data: dict) -> None:
        """成交数据推送"""
        if not self.contract_inited:
            self.trade_data.append(data)
            return

        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map[symbol]

        orderid: str = self.sysid_orderid_map[data["OrderSysID"]]

        timestamp: str = f"{data['TradeDate']} {data['TradeTime']}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S")
        dt: datetime = CHINA_TZ.localize(dt)

        trade: TradeData = TradeData(
            symbol=symbol,
            exchange=contract.exchange,
            orderid=orderid,
            tradeid=data["TradeID"],
            direction=DIRECTION_FUTURES2VT[data["Direction"]],
            offset=OFFSET_FUTURES2VT[data["OffsetFlag"]],
            price=data["Price"],
            volume=data["Volume"],
            datetime=dt,
            gateway_name=self.gateway_name
        )
        self.gateway.on_trade(trade)

    def onRspForQuoteInsert(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """询价请求回报"""
        if not error["ErrorID"]:
            symbol: str = data["InstrumentID"]
            msg: str = f"{symbol}询价请求发送成功"
            self.gateway.write_log(msg)
        else:
            self.gateway.write_error("询价请求发送失败", error)

    def connect(
        self,
        address: str,
        userid: str,
        password: str
    ) -> None:
        """连接服务器"""
        self.userid = userid
        self.password = password

        if not self.connect_status:
            path: Path = get_folder_path(self.gateway_name.lower())
            self.createFtdcTraderApi((str(path) + "\\Td").encode("GBK"))

            self.subscribePrivateTopic(0)
            self.subscribePublicTopic(0)

            self.registerFront(address)
            self.init()

            self.connect_status = True
        else:
            self.authenticate()

    def authenticate(self) -> None:
        """发起授权验证"""
        req: dict = {
            "UserID": self.userid,
            "BrokerID": self.brokerid,
            "AuthCode": self.auth_code,
            "AppID": self.appid
        }

        if self.product_info:
            req["UserProductInfo"] = self.product_info

        self.reqid += 1
        self.reqAuthenticate(req, self.reqid)

    def login(self) -> None:
        """用户登录"""
        if self.login_failed:
            return

        req: dict = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.brokerid,
            "AppID": self.appid
        }

        if self.product_info:
            req["UserProductInfo"] = self.product_info

        self.reqid += 1
        self.reqUserLogin(req, self.reqid)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        if req.offset not in OFFSET_VT2FUTURES:
            self.gateway.write_log("请选择开平方向")
            return ""

        if req.type not in ORDERTYPE_VT2FUTURES:
            self.gateway.write_log(f"当前接口不支持该类型的委托{req.type.value}")
            return ""

        self.order_ref += 1

        nh_req: dict = {
            "InstrumentID": req.symbol,
            "ExchangeID": req.exchange.value,
            "LimitPrice": req.price,
            "VolumeTotalOriginal": int(req.volume),
            "OrderPriceType": ORDERTYPE_VT2FUTURES.get(req.type, ""),
            "Direction": DIRECTION_VT2FUTURES.get(req.direction, ""),
            "CombOffsetFlag": OFFSET_VT2FUTURES.get(req.offset, ""),
            "OrderRef": str(self.order_ref),
            "InvestorID": self.userid,
            "UserID": self.userid,
            "BrokerID": self.brokerid,
            "CombHedgeFlag": THOST_FTDC_HF_Speculation,
            "ContingentCondition": THOST_FTDC_CC_Immediately,
            "ForceCloseReason": THOST_FTDC_FCC_NotForceClose,
            "IsAutoSuspend": 0,
            "TimeCondition": THOST_FTDC_TC_GFD,
            "VolumeCondition": THOST_FTDC_VC_AV,
            "MinVolume": 1
        }

        if req.type == OrderType.FAK:
            nh_req["OrderPriceType"] = THOST_FTDC_OPT_LimitPrice
            nh_req["TimeCondition"] = THOST_FTDC_TC_IOC
            nh_req["VolumeCondition"] = THOST_FTDC_VC_AV
        elif req.type == OrderType.FOK:
            nh_req["OrderPriceType"] = THOST_FTDC_OPT_LimitPrice
            nh_req["TimeCondition"] = THOST_FTDC_TC_IOC
            nh_req["VolumeCondition"] = THOST_FTDC_VC_CV

        self.reqid += 1
        self.reqOrderInsert(nh_req, self.reqid)

        orderid: str = f"{self.frontid}_{self.sessionid}_{self.order_ref}"
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        frontid, sessionid, order_ref = req.orderid.split("_")

        nh_req: dict = {
            "InstrumentID": req.symbol,
            "ExchangeID": req.exchange.value,
            "OrderRef": order_ref,
            "FrontID": int(frontid),
            "SessionID": int(sessionid),
            "ActionFlag": THOST_FTDC_AF_Delete,
            "BrokerID": self.brokerid,
            "InvestorID": self.userid
        }

        self.reqid += 1
        self.reqOrderAction(nh_req, self.reqid)

    def send_rfq(self, req: OrderRequest) -> str:
        """询价请求"""
        self.order_ref += 1

        nh_req: dict = {
            "InstrumentID": req.symbol,
            "ExchangeID": req.exchange.value,
            "ForQuoteRef": str(self.order_ref),
            "BrokerID": self.brokerid,
            "InvestorID": self.userid
        }

        self.reqid += 1
        self.reqForQuoteInsert(nh_req, self.reqid)

        orderid: str = f"{self.frontid}_{self.sessionid}_{self.order_ref}"
        vt_orderid: str = f"{self.gateway_name}.{orderid}"

        return vt_orderid

    def query_account(self) -> None:
        """查询资金"""
        self.reqid += 1
        self.reqQryTradingAccount({}, self.reqid)

    def query_position(self) -> None:
        """查询持仓"""
        if not symbol_contract_map:
            return

        req: dict = {
            "BrokerID": self.brokerid,
            "InvestorID": self.userid
        }

        self.reqid += 1
        self.reqQryInvestorPosition(req, self.reqid)

    def close(self) -> None:
        """关闭接口"""
        if self.connect_status:
            self.exit()


class NhStockTdApi(StockTdApi):
    """"""

    def __init__(self, gateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.reqid: int = 0

        prefix: str = datetime.now().strftime("%H%M%S0000")
        self.order_ref: int = int(prefix)

        self.connect_status: bool = False
        self.login_status: bool = False
        self.auth_status: bool = False
        self.login_failed: bool = False
        self.contract_inited: bool = False

        self.userid: str = ""
        self.password: str = ""
        self.auth_code: str = ""
        self.appid: str = ""
        self.product_info: str = ""

        self.party_id: str = ""

        self.today_date: str = ""
        self.order_data: List[dict] = []
        self.trade_data: List[dict] = []

        self.orders: Dict[str, OrderData] = {}

        self.instrument_countdown: int = 0

    def connect(
        self,
        address: str,
        userid: str,
        password: str,
        party_id: str,
        appid: str,
        auth_code: str,
    ) -> None:
        """连接服务器"""
        self.userid = userid
        self.password = password
        self.party_id = party_id
        self.appid = appid
        self.auth_code = auth_code

        if not self.connect_status:
            path: Path = get_folder_path(self.gateway_name.lower())
            self.createStockTdApi((str(path) + "\\Td").encode("GBK"))

            self.subscribePrivateTopic(0)
            self.subscribePublicTopic(0)
            self.subscribeUserTopic(0)

            self.registerFront(address)
            self.init("", "")

            self.connect_status = True
        else:
            self.login()

    def login(self) -> None:
        """用户登录"""
        if self.login_failed:
            return

        req: dict = {
            "UserID": self.userid,
            "Password": self.password,
            "UserProductInfo": self.appid,
            "InterfaceProductInfo": self.auth_code
        }

        self.reqid += 1
        self.reqUserLogin(req, self.reqid)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        if req.offset not in OFFSET_VT2STOCK:
            self.gateway.write_log("请选择开平方向")
            return ""

        if req.type not in ORDERTYPE_VT2STOCK:
            self.gateway.write_log(f"当前接口不支持该类型的委托{req.type.value}")
            return ""

        self.order_ref += 1
        ord_type, time_in_force = ORDERTYPE_VT2STOCK[req.type]

        nh_req: dict = {
            "SecurityID": req.symbol,
            "Price": req.price,
            "OrderQty": int(req.volume),
            "OrdType": ord_type,
            "Side": DIRECTION_VT2STOCK[req.direction],
            "PositionEffect": OFFSET_VT2STOCK[req.offset],
            "TimeInForce": time_in_force,
            "ClOrdID": self.order_ref,
            "PartyID": self.party_id,
            "CoveredOrUncovered": 1,
            "OwnerType": 1
        }

        self.reqid += 1
        self.reqOptionsInsert(nh_req, self.reqid)

        orderid: str = str(self.order_ref)
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.orders[orderid] = order

        self.gateway.on_order(order)
        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.order_ref += 1

        nh_req: dict = {"ClOrdID": self.order_ref}

        if req.orderid in self.orders:
            nh_req["OrigClOrdID"] = int(req.orderid)
        else:
            nh_req["OrderID"] = req.orderid

        self.reqid += 1
        self.reqOptionsCancel(nh_req, self.reqid)

    def query_instrument(self, event: Event) -> None:
        """查询合约"""
        self.instrument_countdown -= 1
        if self.instrument_countdown:
            return
        self.gateway.event_engine.unregister(EVENT_TIMER, self.query_instrument)

        self.reqid += 1
        self.reqQryOptions({}, self.reqid)

    def query_client(self) -> None:
        """查询投资者账户"""
        self.reqid += 1
        self.reqQryClient(self.reqid)

    def query_account(self) -> None:
        """查询资金"""
        if not self.party_id:
            return
        req: dict = {"PartyID": self.party_id}

        self.reqid += 1
        self.reqQryPartAccount(req, self.reqid)

    def query_position(self) -> None:
        """查询持仓"""
        if not self.party_id:
            return
        req: dict = {"PartyID": self.party_id}

        self.reqid += 1
        self.reqQryPosition(req, self.reqid)

    def close(self) -> None:
        """关闭连接"""
        if self.connect_status:
            self.exit()

    def send_exercise(self, req: dict) -> None:
        """委托行权"""
        self.order_ref += 1
        self.reqid += 1

        leg1_symbol: str = req.get("leg1_symbol", "")
        leg2_symbol: str = req.get("leg2_symbol", "")

        # 单腿行权
        if not leg2_symbol:
            nh_req: dict = {
                "ClOrdID": self.order_ref,
                "SecurityID": req["leg1_symbol"],
                "OwnerType": 1,
                "OrderQty": req["volume"],
                "PartyID": self.party_id,
            }
            self.reqExercise(nh_req, self.reqid)
        # 组合行权
        else:
            nh_req: dict = {
                "ClOrdID": self.order_ref,
                "OwnerType": 1,
                "OrderQty": req["volume"],
                "PartyID": self.party_id,
                "LegSecurityID1": leg1_symbol,
                "LegOrderQty1": req["volume"],
                "LegSecurityID2": leg2_symbol,
                "LegOrderQty2": req["volume"],
            }
            self.reqCombExercise(nh_req, self.reqid)

    def cancel_exercise(self, req: CancelRequest) -> None:
        """撤销行权"""
        self.reqid += 1
        nh_req: dict = {"OrderID": req.orderid}
        self.reqExerciseCancel(nh_req, self.reqid)

    def query_exercise(self) -> None:
        """查询行权记录"""
        self.reqid += 1
        nh_req: dict = {"PartyID": self.party_id}
        self.reqQryExercise(nh_req, self.reqid)

    def onFrontConnected(self) -> None:
        """服务器连接成功回报"""
        self.gateway.write_log("交易服务器连接成功")
        self.connect_status = True

        if not self.login_status:
            self.login()

    def onFrontDisconnected(self, reqid: int) -> None:
        """服务器连接断开回报"""
        self.login_status = False
        self.gateway.write_log(f"交易服务器连接断开，原因{reqid}")

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """用户登录请求回报"""
        if not error["ErrorID"]:
            self.login_status = True
            self.gateway.write_log("交易服务器登录成功")

            self.order_ref: int = max(self.order_ref, data["MaxClOrdID"])
            self.today_date: str = data["TradingDay"]

            self.instrument_countdown: int = 10
            self.gateway.event_engine.register(EVENT_TIMER, self.query_instrument)
        else:
            self.login_failed = True
            self.gateway.write_error("交易服务器登录失败", error)

    def onRspOptionsInsert(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """委托下单失败回报"""
        if error["ErrorID"]:
            orderid: str = str(data["ClOrdID"])
            order: OrderData = self.orders[orderid]
            order.status = Status.REJECTED
            self.gateway.on_order(copy(order))

            self.gateway.write_error("交易委托失败", error)

    def onRspOptionsCancel(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """委托撤单失败回报"""
        if error["ErrorID"]:
            self.gateway.write_error("交易撤单失败", error)

    def onRspExercise(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """委托行权回报"""
        if error["ErrorID"]:
            msg: str = error["ErrorMsg"]
            self.gateway.on_event(EVENT_NH_EXERCISE_LOG, msg)
            return

        exercise: dict = {
            "orderid": data["OrderID"],
            "volume": data["OrderQty"],
            "active": True,
            "leg1_symbol": data["SecurityID"],
        }
        self.gateway.on_event(EVENT_NH_EXERCISE, exercise)

    def onRspExerciseCancel(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """撤销行权回报"""
        if error["ErrorID"]:
            msg: str = error["ErrorMsg"]
            self.gateway.on_event(EVENT_NH_EXERCISE_LOG, msg)
            return

        exercise: dict = {
            "orderid": data["OrderID"],
            "volume": data["OrderQty"],
            "active": False,
            "leg1_symbol": data["SecurityID"],
        }
        self.gateway.on_event(EVENT_NH_EXERCISE, exercise)

    def onRspQryPartAccount(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """资金查询回报"""
        if data["PartyID"]:
            account: AccountData = AccountData(
                accountid=data["PartyID"],
                balance=data["Balance"],
                frozen=data["FrozenMargin"] + data["FrozenPremium"] + data["FrozenCommi"],
                gateway_name=self.gateway_name
            )
            account.available = data["Available"]
            self.gateway.on_account(account)

    def onRspQryPosition(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """持仓查询回报"""

        # 必须已经收到了合约信息后才能处理
        symbol: str = data["SecurityID"]
        contract: ContractData = symbol_contract_map.get(symbol, None)

        if contract:
            size: int = contract.size
            price: float = data["PositionCost"] / (data["Position"] * size)

            position: PositionData = PositionData(
                symbol=data["SecurityID"],
                exchange=contract.exchange,
                direction=DIRECTION_STOCK2VT[data["Side"]],
                volume=data["Position"],
                yd_volume=data["YdPosition"],
                price=price,
                gateway_name=self.gateway_name
            )

            self.gateway.on_position(position)

    def onRspQryOptions(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """合约查询回报"""
        if data["contractid"]:
            if data["contractid"].startswith("9"):
                exchange: Exchange = Exchange.SZSE
            else:
                exchange: Exchange = Exchange.SSE

            contract: ContractData = ContractData(
                symbol=data["contractid"],
                exchange=exchange,
                name=data["contractsymbol"],
                product=Product.OPTION,
                size=data["contractmultiplierunit"],
                pricetick=data["ticksize"],
                option_portfolio=data["underlyingsecurityid"],
                option_underlying=data["underlyingsecurityid"] + "_" + data["expiredate"],
                option_type=OPTIONTYPE_STOCK2VT[data["callorput"]],
                option_strike=data["exerciseprice"],
                option_expiry=datetime.strptime(data["expiredate"], "%Y%m%d"),
                gateway_name=self.gateway_name
            )

            contract.option_index = get_option_index(
                contract.option_strike,
                data["contractsymbol"]
            )

            self.gateway.on_contract(contract)

            symbol_contract_map[contract.symbol] = contract

        if last:
            self.contract_inited = True
            self.gateway.write_log("合约信息查询成功")

            for data in self.order_data:
                self.onRtnOptionsOrder(data)
            self.order_data.clear()

            for data in self.trade_data:
                self.onRtnOptionsTrade(data)
            self.trade_data.clear()

    def onRtnOptionsOrder(self, data: dict) -> None:
        """委托更新推送"""
        if not self.contract_inited:
            self.order_data.append(data)
            return

        symbol: str = data["SecurityID"]
        contract: ContractData = symbol_contract_map[symbol]
        orderid: str = str(data["ClOrdID"])

        if orderid not in self.orders:
            orderid: str = data["OrderID"]

        timestamp: str = f"{self.today_date} {data['TransactTimeOnly']}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H%M%S")
        dt: datetime = CHINA_TZ.localize(dt)

        order_type: OrderType = ORDERTYPE_STOCK2VT[(data["OrdType"], data["TimeInForce"])]

        order: OrderData = OrderData(
            symbol=symbol,
            exchange=contract.exchange,
            orderid=orderid,
            type=order_type,
            direction=DIRECTION_STOCK2VT[data["Side"]],
            offset=OFFSET_STOCK2VT[data["PositionEffect"]],
            price=data["Price"],
            volume=data["OrderQty"],
            traded=data["TradeQty"],
            status=STATUS_STOCK2VT[data["OrdStatus"]],
            datetime=dt,
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)

    def onRtnOptionsTrade(self, data: dict) -> None:
        """成交更新推送"""
        if not self.contract_inited:
            self.trade_data.append(data)
            return

        symbol: str = data["SecurityID"]
        contract: ContractData = symbol_contract_map[symbol]
        orderid: str = str(data["ClOrdID"])

        timestamp: str = f"{self.today_date} {data['TransactTimeOnly']}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H%M%S")
        dt: datetime = CHINA_TZ.localize(dt)

        trade: TradeData = TradeData(
            symbol=symbol,
            exchange=contract.exchange,
            orderid=orderid,
            tradeid=data["ExecID"],
            direction=DIRECTION_STOCK2VT[data["Side"]],
            offset=OFFSET_STOCK2VT[data["PositionEffect"]],
            price=data["LastPx"],
            volume=data["LastQty"],
            datetime=dt,
            gateway_name=self.gateway_name
        )
        self.gateway.on_trade(trade)

    def onRtnExercise(self, data: dict) -> None:
        """行权更新推送"""
        exercise: dict = {
            "orderid": data["OrderID"],
            "volume": data["OrderQty"],
            "active": True,
            "leg1_symbol": data["SecurityID"],
        }
        self.gateway.on_event(EVENT_NH_EXERCISE, exercise)

    def onRspQryExercise(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """行权记录查询回报"""
        if error["ErrorID"]:
            msg: str = error["ErrorMsg"]
            self.gateway.on_event(EVENT_NH_EXERCISE_LOG, msg)
            return

        if data["OrderID"]:
            exercise: dict = {
                "orderid": data["OrderID"],
                "volume": data["OrderQty"],
                "active": True,
                "leg1_symbol": data["SecurityID"],
            }
            self.gateway.on_event(EVENT_NH_EXERCISE, exercise)

    def onRspCombExercise(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """组合行权回报"""
        exercise: dict = {
            "orderid": data["OrderID"],
            "volume": data["OrderQty"],
            "active": True,
            "leg1_symbol": data["LegSecurityID1"],
            "leg2_symbol": data["LegSecurityID2"],
        }
        self.gateway.on_event(EVENT_NH_EXERCISE, exercise)


def adjust_price(price: float) -> float:
    """将异常的浮点数最大值（MAX_FLOAT）数据调整为0"""
    if price == MAX_FLOAT:
        price = 0
    return price


def get_option_index(strike_price: float, exchange_instrument_id: str) -> str:
    """获取期权索引"""
    exchange_instrument_id = exchange_instrument_id.replace(" ", "")

    if "M" in exchange_instrument_id:
        n = exchange_instrument_id.index("M")
    elif "A" in exchange_instrument_id:
        n = exchange_instrument_id.index("A")
    elif "B" in exchange_instrument_id:
        n = exchange_instrument_id.index("B")
    else:
        return str(strike_price)

    index = exchange_instrument_id[n:]
    option_index = f"{strike_price:.3f}-{index}"

    return option_index
