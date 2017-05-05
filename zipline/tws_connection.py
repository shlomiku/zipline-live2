from collections import namedtuple
from datetime import datetime

from ib.ext.EClientSocket import EClientSocket
from ib.ext.EWrapper import EWrapper
from ib.ext.Contract import Contract

from logbook import Logger

log = Logger('TWS Connection')

LOG_MESSAGES = True

RTVolumeBar = namedtuple('RTVolumeBar', ['last_trade_price',
                                         'last_trade_size',
                                         'last_trade_time',
                                         'total_volume',
                                         'vwap',
                                         'single_trade_flag'])

def log_message(message, mapping):
    if not LOG_MESSAGES:
        return
    else:
        try:
            del(mapping['self'])
        except (KeyError, ):
            pass
        items = list(mapping.items())
        items.sort()
        log.info(('### %s' % (message, )))
        for k, v in items:
            log.info(('    %s:%s' % (k, v)))


class TWSConnection(EClientSocket, EWrapper):
    def __init__(self, tws_uri):
        EWrapper.__init__(self)
        EClientSocket.__init__(self, anyWrapper=self)

        self.tws_uri = tws_uri
        host, port, client_id = self.tws_uri.split(':')

        self._next_ticker_id = 0

        log.info("Initiating TWS Connection to: %s:%s:%s" % (host, int(port), int(client_id)))
        self.eConnect(host, int(port), int(client_id))
        log.info("Done")

        self.__marketDataTickerIDs = {}
        self.__currentTicks = {}
        self.__marketData = {}

        # self.reqManagedAccts()
        # self.reqAccountUpdates(1, '')
        # self.subscribe_market_data('AAPL')


    @property
    def next_ticker_id(self):
        ticker_id = self._next_ticker_id
        self._next_ticker_id += 1
        return ticker_id

    def subscribe_market_data(self, symbol, sec_type='STK', exchange='SMART', currency='USD'):
        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = sec_type
        contract.m_exchange = exchange
        contract.m_currency = currency

        ticker_id = self.next_ticker_id

        self.__marketDataTickerIDs[ticker_id] = symbol

        tick_list = "233"  # RTVolume
        self.reqMktData(self.next_ticker_id, contract, tick_list, False)

    def __process_tick(self, tickerId, tickType, value):
        if tickType not in (4, 8, 45, 46, 48, 54):
            return

        instr = self.__marketDataTickerIDs[tickerId]

        if tickType == 45:  # LAST_TIMESTAMP
            dt = datetime.utcfromtimestamp(int(value))
            self.__currentTicks[instr].dt = dt
        elif tickType == 4:  # LAST_PRICE
            self.__currentTicks[instr].price = float(value)
        elif tickType == 8:  # VOLUME
            self.__currentTicks[instr].volume = int(value)
        elif tickType == 54:  # TRADE_COUNT
            self.__currentTicks[instr].tradeCount = int(value)
        elif tickType == 46:  # SHORTABLE
            self.__currentTicks[instr].shortable = float(value)
        elif tickType == 48:
            # Format:
            # Last trade price; Last trade size;Last trade time;Total volume;VWAP;Single trade flag
            # e.g.: 701.28;1;1348075471534;67854;701.46918464;true
            (lastTradePrice, lastTradeSize, lastTradeTime, totalVolume, VWAP, singleTradeFlag) = value.split(';')

            # Ignore if lastTradePrice is empty:
            # tickString: tickerId=0 tickType=48/RTVolume ;0;1469805548873;240304;216.648653;true
            if len(lastTradePrice) == 0:
                return

            rt_volume_bar = RTVolumeBar(last_trade_price=float(lastTradePrice),
                                        last_trade_size=int(lastTradeSize),
                                        last_trade_time=float(lastTradeTime) / 1000, # Convert to microsecond based utc
                                        total_volume=int(totalVolume),
                                        vwap=float(VWAP),
                                        single_trade_flag=singleTradeFlag
            )
            log.info(rt_volume_bar)
            self.__marketData[instr].append(rt_volume_bar)

    def tickPrice(self, tickerId, field, price, canAutoExecute):
        log_message('tickPrice', vars())
        self.__process_tick(tickerId, tickType=field, value=price)

    def tickSize(self, tickerId, field, size):
        log_message('tickSize', vars())
        self.__process_tick(tickerId, tickType=field, value=size)

    def tickOptionComputation(self, tickerId, field, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta,
                              undPrice):
        log_message('tickOptionComputation', vars())

    def tickGeneric(self, tickerId, tickType, value):
        log_message('tickGeneric', vars())
        self.__process_tick(tickerId, tickType=tickType, value=value)

    def tickString(self, tickerId, tickType, value):
        log_message('tickString', vars())
        self.__process_tick(tickerId, tickType=tickType, value=value)

    def tickEFP(self, tickerId, tickType, basisPoints, formattedBasisPoints, impliedFuture, holdDays, futureExpiry,
                dividendImpact, dividendsToExpiry):
        log_message('tickEFP', vars())

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId,
                    whyHeId):
        log_message('orderStatus', vars())

    def openOrder(self, orderId, contract, order, state):
        log_message('openOrder', vars())

    def openOrderEnd(self):
        log_message('openOrderEnd', vars())

    def updateAccountValue(self, key, value, currency, accountName):
        log_message('updateAccountValue', vars())

    def updatePortfolio(self, contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL,
                        accountName):
        log_message('updatePortfolio', vars())

    def updateAccountTime(self, timeStamp):
        log_message('updateAccountTime', vars())

    def accountDownloadEnd(self, accountName):
        log_message('accountDownloadEnd', vars())

    def nextValidId(self, orderId):
        log_message('nextValidId', vars())

    def contractDetails(self, reqId, contractDetails):
        log_message('contractDetails', vars())

    def contractDetailsEnd(self, reqId):
        log_message('contractDetailsEnd', vars())

    def bondContractDetails(self, reqId, contractDetails):
        log_message('bondContractDetails', vars())

    def execDetails(self, reqId, contract, execution):
        log_message('execDetails', vars())

    def execDetailsEnd(self, reqId):
        log_message('execDetailsEnd', vars())

    def connectionClosed(self):
        log_message('connectionClosed', {})

    def error(self, id=None, errorCode=None, errorMsg=None):
        log_message('error', vars())

    def updateMktDepth(self, tickerId, position, operation, side, price, size):
        log_message('updateMktDepth', vars())

    def updateMktDepthL2(self, tickerId, position, marketMaker, operation, side, price, size):
        log_message('updateMktDepthL2', vars())

    def updateNewsBulletin(self, msgId, msgType, message, origExchange):
        log_message('updateNewsBulletin', vars())

    def managedAccounts(self, accountsList):
        log_message('managedAccounts', vars())

    def receiveFA(self, faDataType, xml):
        log_message('receiveFA', vars())

    def historicalData(self, reqId, date, open, high, low, close, volume, count, WAP, hasGaps):
        log_message('historicalData', vars())

    def scannerParameters(self, xml):
        log_message('scannerParameters', vars())

    def scannerData(self, reqId, rank, contractDetails, distance, benchmark, projection, legsStr):
        log_message('scannerData', vars())

    def commissionReport(self, commissionReport):
        log_message('commissionReport', vars())

    def currentTime(self, time):
        log_message('currentTime', vars())

    def deltaNeutralValidation(self, reqId, underComp):
        log_message('deltaNeutralValidation', vars())

    def fundamentalData(self, reqId, data):
        log_message('fundamentalData', vars())

    def marketDataType(self, reqId, marketDataType):
        log_message('marketDataType', vars())

    def realtimeBar(self, reqId, time, open, high, low, close, volume, wap, count):
        log_message('realtimeBar', vars())

    def scannerDataEnd(self, reqId):
        log_message('scannerDataEnd', vars())

    def tickSnapshotEnd(self, reqId):
        log_message('tickSnapshotEnd', vars())

    def position(self, account, contract, pos, avgCost):
        log_message('position', vars())

    def positionEnd(self):
        log_message('positionEnd', vars())

    def accountSummary(self, reqId, account, tag, value, currency):
        log_message('accountSummary', vars())

    def accountSummaryEnd(self, reqId):
        log_message('accountSummaryEnd', vars())
