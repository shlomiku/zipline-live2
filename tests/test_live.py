"""
Tests for live trading.
"""
from unittest import TestCase
from datetime import time
from collections import defaultdict

import pandas as pd
import numpy as np

# fix to allow zip_longest on Python 2.X and 3.X
try:                                    # Python 3
    from itertools import zip_longest
except ImportError:                     # Python 2
    from itertools import izip_longest as zip_longest

from functools import partial

import os
from math import fabs

from mock import patch, sentinel, Mock, MagicMock
from testfixtures import tempdir

from ib.ext.Contract import Contract
from ib.ext.Order import Order
from ib.ext.Execution import Execution
from ib.ext.OrderState import OrderState

import alpaca_trade_api.rest as apca

from zipline.algorithm import TradingAlgorithm
from zipline.algorithm_live import LiveTradingAlgorithm, LiveAlgorithmExecutor
from zipline.data.data_portal_live import DataPortalLive
from zipline.gens.realtimeclock import (RealtimeClock,
                                        SESSION_START,
                                        BEFORE_TRADING_START_BAR)
from zipline.finance.order import Order as ZPOrder
from zipline.finance.blotter_live import BlotterLive
from zipline.gens.sim_engine import MinuteSimulationClock
from zipline.gens.brokers.broker import Broker
from zipline.gens.brokers.ib_broker import IBBroker, TWSConnection
from zipline.gens.brokers.alpaca_broker import ALPACABroker
from zipline.testing.fixtures import WithSimParams
from zipline.finance.execution import (StopLimitOrder,
                                       MarketOrder,
                                       StopOrder,
                                       LimitOrder)
from zipline.finance.order import ORDER_STATUS
from zipline.finance.transaction import Transaction
from zipline.utils.calendars import get_calendar
from zipline.utils.calendars.trading_calendar import days_at_time
from zipline.utils.serialization_utils import load_context, store_context
from zipline.testing.fixtures import (ZiplineTestCase,
                                      WithTradingEnvironment,
                                      WithDataPortal)
from zipline.errors import CannotOrderDelistedAsset


class TestRealtimeClock(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.nyse_calendar = get_calendar("NYSE")

        cls.sessions = cls.nyse_calendar.sessions_in_range(
            pd.Timestamp("2017-04-20"),
            pd.Timestamp("2017-04-20")
        )

        trading_o_and_c = cls.nyse_calendar.schedule.ix[cls.sessions]
        cls.opens = trading_o_and_c['market_open']
        cls.closes = trading_o_and_c['market_close']

    def setUp(self):
        self.internal_clock = None
        self.events = defaultdict(list)

    def advance_clock(self, x):
        """Mock function for sleep. Advances the internal clock by 1 min"""
        # The internal clock advance time must be 1 minute to match
        # MinutesSimulationClock's update frequency
        self.internal_clock += pd.Timedelta('1 min')

    def get_clock(self, arg, *args, **kwargs):
        """Mock function for pandas.to_datetime which is used to query the
        current time in RealtimeClock"""
        assert arg == "now"
        return self.internal_clock

    def test_crosscheck_realtimeclock_with_minutesimulationclock(self):
        """Tests that RealtimeClock behaves like MinuteSimulationClock"""
        for minute_emission in (False, True):
            # MinuteSimulationClock also relies on to_datetime, shall not be
            # created in the patch block
            msc = MinuteSimulationClock(
                self.sessions,
                self.opens,
                self.closes,
                days_at_time(self.sessions, time(8, 45), "US/Eastern"),
                minute_emission
            )
            msc_events = list(msc)

            with patch('zipline.gens.realtimeclock.pd.to_datetime') as to_dt, \
                    patch('zipline.gens.realtimeclock.sleep') as sleep:
                rtc = iter(RealtimeClock(
                    self.sessions,
                    self.opens,
                    self.closes,
                    days_at_time(self.sessions, time(8, 45), "US/Eastern"),
                    minute_emission
                ))
                self.internal_clock = \
                    pd.Timestamp("2017-04-20 00:00", tz='UTC')
                to_dt.side_effect = self.get_clock
                sleep.side_effect = self.advance_clock

                rtc_events = list(rtc)

            for rtc_event, msc_event in zip_longest(rtc_events, msc_events):
                self.assertEquals(rtc_event, msc_event)

            self.assertEquals(len(rtc_events), len(msc_events))

    def test_time_skew(self):
        """Tests that RealtimeClock's time_skew parameter behaves as
        expected"""
        for time_skew in (pd.Timedelta("2 hour"), pd.Timedelta("-120 sec")):
            with patch('zipline.gens.realtimeclock.pd.to_datetime') as to_dt, \
                    patch('zipline.gens.realtimeclock.sleep') as sleep:
                clock = RealtimeClock(
                    self.sessions,
                    self.opens,
                    self.closes,
                    days_at_time(self.sessions, time(11, 31), "US/Eastern"),
                    False,
                    time_skew
                )
                to_dt.side_effect = self.get_clock
                sleep.side_effect = self.advance_clock
                start_time = pd.Timestamp("2017-04-20 15:31", tz='UTC')
                self.internal_clock = start_time

                events = list(clock)

                # Event 0 is SESSION_START which always happens at 00:00.
                ts, event_type = events[1]
                self.assertEquals(ts, start_time + time_skew)

    def test_midday_start(self):
        """Tests that RealtimeClock is able to execute if started mid-day"""
        msc = MinuteSimulationClock(
            self.sessions,
            self.opens,
            self.closes,
            days_at_time(self.sessions, time(8, 45), "US/Eastern"),
            False
        )
        msc_events = list(msc)

        with patch('zipline.gens.realtimeclock.pd.to_datetime') as to_dt, \
                patch('zipline.gens.realtimeclock.sleep') as sleep:
            rtc = RealtimeClock(
                self.sessions,
                self.opens,
                self.closes,
                days_at_time(self.sessions, time(8, 45), "US/Eastern"),
                False
            )

            to_dt.side_effect = self.get_clock
            sleep.side_effect = self.advance_clock
            self.internal_clock = pd.Timestamp("2017-04-20 15:00", tz='UTC')

            rtc_events = list(rtc)

        # Count the mid-day position in the MinuteSimulationClock's events:
        # Simulation Tick: 2017-04-20 00:00:00+00:00 - 1 (SESSION_START)
        # Simulation Tick: 2017-04-20 12:45:00+00:00 - 4 (BEFORE_TRADING_START)
        # Simulation Tick: 2017-04-20 13:31:00+00:00 - 0 (BAR)
        msc_midday_position = 2 + 90
        self.assertEquals(rtc_events[0], msc_events[0])  # Session start bar

        # before_trading_start is fired immediately if we're after 8:45 EDT
        event_time, event_type = rtc_events[1]
        self.assertEquals(event_time,
                          pd.Timestamp("2017-04-20 15:00", tz='UTC'))
        self.assertEquals(event_type, BEFORE_TRADING_START_BAR)

        self.assertEquals(rtc_events[2:], msc_events[msc_midday_position:])

    def test_afterhours_start(self):
        """Tests that RealtimeClock returns immediately if started after RTH"""
        with patch('zipline.gens.realtimeclock.pd.to_datetime') as to_dt, \
                patch('zipline.gens.realtimeclock.sleep') as sleep:
            rtc = RealtimeClock(
                self.sessions,
                self.opens,
                self.closes,
                days_at_time(self.sessions, time(8, 45), "US/Eastern"),
                False
            )

            to_dt.side_effect = self.get_clock
            sleep.side_effect = self.advance_clock
            self.internal_clock = pd.Timestamp("2017-04-20 20:05", tz='UTC')

            events = list(rtc)
            self.assertEquals(len(events), 2)

            # SESSION_START & which always triggered.
            _, event_type = events[0]
            self.assertEquals(event_type, SESSION_START)

            event_time, event_type = events[1]
            self.assertEquals(event_time,
                              pd.Timestamp("2017-04-20 20:05", tz='UTC'))
            self.assertEquals(event_type, BEFORE_TRADING_START_BAR)


class TestPersistence(WithSimParams, WithTradingEnvironment, ZiplineTestCase):
    def noop(*args, **kwargs):
        pass

    def make_trading_algo(self, state_filename, algo_filename=None,
                          initialize=noop, handle_data=noop):
        return LiveTradingAlgorithm(
            namespace={},
            env=self.make_trading_environment(),
            get_pipeline_loader=self.make_load_function(),
            sim_params=self.make_simparams(),
            state_filename=state_filename,
            algo_filename=algo_filename,
            initialize=initialize,
            handle_data=handle_data,
            script=None)

    @tempdir()
    def test_live_trading_algorithm_creates_state_file(self, tmpdir):
        algo_text = b"""
        def initialize(context):
            pass

        def handle_data(context, data):
            pass
        """
        algo_filename = "algo.py"
        algo_path = tmpdir.write(algo_filename, algo_text)
        state_filename = os.path.join(tmpdir.path, "state_file")

        algo = self.make_trading_algo(state_filename, algo_path)

        assert not os.path.exists(state_filename)

        algo.initialize()

        assert os.path.getsize(state_filename) > 0

    @tempdir()
    def test_live_trading_algorithm_loads_state_file(self, tmpdir):
        state_filename = os.path.join(tmpdir.path, "state_file")

        def initialize_1(context):
            context.state_from_initialize = 7

        def handle_data_1(context, data):
            context.state_from_handle_data = 11

        algo_1 = self.make_trading_algo(state_filename,
                                        initialize=initialize_1,
                                        handle_data=handle_data_1)

        algo_1.initialize()
        algo_1.handle_data(data=sentinel.data)

        def initialize_2(context):
            assert False, "initialize shouldn't be called if state is loaded"

        def handle_data_2(context, data):
            assert False, "handle_data shouldn't be called"

        algo_2 = self.make_trading_algo(state_filename,
                                        initialize=initialize_2,
                                        handle_data=handle_data_2)
        algo_2.initialize()

        assert algo_2.state_from_initialize == 7
        assert algo_2.state_from_handle_data == 11

    @tempdir()
    def test_state_load_with_corrupt_state(self, tmpdir):
        state_filename = os.path.join(tmpdir.path, "state_file")

        algo_1 = self.make_trading_algo(state_filename,
                                        initialize=TestPersistence.noop,
                                        handle_data=TestPersistence.noop)

        tmpdir.write("state_file", b"roken")

        with self.assertRaises(ValueError) as e:
            algo_1.initialize()
        assert "state file" in str(e.exception)

    @tempdir()
    def test_context_persistence_checksum(self, tmpdir):
        algo_text_1 = b"""
        def initialize(context):
            context.state_from_initialize = 11

        def handle_data(context, data):
            context.state_from_handle_data = 13
        """
        algo_filename_1 = "algo_1.py"
        algo_path_1 = tmpdir.write(algo_filename_1, algo_text_1)

        state_filename_1 = os.path.join(tmpdir.path, "state_file_1")
        algo_1 = self.make_trading_algo(state_filename_1,
                                        algo_filename=algo_path_1)

        algo_1.initialize()
        algo_1.handle_data(data=sentinel.data)

        algo_text_2 = b"""
        def initialize(context):
            context.state_from_initialize = 7

        def handle_data(context, data):
            context.state_from_handle_data = 5
        """
        algo_filename_2 = "algo_2.py"
        algo_path_2 = tmpdir.write(algo_filename_2, algo_text_2)

        state_filename_2 = os.path.join(tmpdir.path, "state_file_2")
        algo_2 = self.make_trading_algo(state_filename_2,
                                        algo_filename=algo_path_2)

        algo_2.initialize()
        algo_2.handle_data(data=sentinel.data)

        algo_1_wrong_state = self.make_trading_algo(state_filename_2,
                                                    algo_filename=algo_path_1)

        algo_2_wrong_state = self.make_trading_algo(state_filename_1,
                                                    algo_filename=algo_path_2)

        with self.assertRaises(TypeError) as e1:
            algo_1_wrong_state.initialize()
        assert "state file" in str(e1.exception)

        with self.assertRaises(TypeError) as e2:
            algo_2_wrong_state.initialize()
        assert "state file" in str(e2.exception)

    @tempdir()
    def test_context_persistence_exclude_list(self, tmpdir):
        class Context(object):
            def __init__(self, rsi=None, sma=None,
                         trading_client=None, event_manager=None):
                self.rsi = rsi
                self.sma = sma
                self.trading_client = trading_client
                self.event_manager = event_manager

        context = Context(rsi=17.2, sma=40.4, trading_client=lambda x: x + 3,
                          event_manager=[None, False])

        exclude_list = ['trading_client', 'event_manager']
        checksum = 'robocop'

        state_file_path = os.path.join(tmpdir.path, "state_file")

        store_context(state_file_path, context, checksum, exclude_list)

        restored_context = Context()
        load_context(state_file_path, restored_context, checksum)

        assert restored_context.__dict__.keys() == context.__dict__.keys()
        assert restored_context.rsi == context.rsi
        assert restored_context.sma == context.sma
        assert restored_context.trading_client is None
        assert restored_context.event_manager is None


class TestLiveTradingAlgorithm(WithSimParams,
                               WithDataPortal,
                               WithTradingEnvironment,
                               ZiplineTestCase):
    ASSET_FINDER_EQUITY_SIDS = (1, 2)
    ASSET_FINDER_EQUITY_SYMBOLS = ("SPY", "XIV")
    START_DATE = pd.to_datetime('2017-01-03', utc=True)
    END_DATE = pd.to_datetime('2017-04-26', utc=True)
    SIM_PARAMS_DATA_FREQUENCY = 'minute'
    SIM_PARAMS_EMISSION_RATE = 'minute'

    def test_live_trading_supports_orders_outside_ingested_period(self):
        def create_initialized_algo(trading_algorithm_class, current_dt):
            def initialize(context):
                pass

            def handle_data(context, data):
                context.order_value(context.symbol("SPY"), 100)

            algo = trading_algorithm_class(
                namespace={},
                env=self.make_trading_environment(),
                get_pipeline_loader=self.make_load_function(),
                sim_params=self.make_simparams(),
                state_filename='blah',
                algo_filename='foo',
                initialize=initialize,
                handle_data=handle_data,
                script=None)

            algo.initialize()
            algo.initialized = True  # Normally this is set through algo.run()
            algo.datetime = current_dt

            return algo

        current_dt = self.END_DATE + pd.Timedelta("1 day")

        backtest_algo = create_initialized_algo(TradingAlgorithm, current_dt)

        with self.assertRaises(CannotOrderDelistedAsset):
            backtest_algo.handle_data(data=sentinel.data)

        broker = MagicMock(spec=Broker)
        live_algo = create_initialized_algo(
            partial(LiveTradingAlgorithm, broker=broker), current_dt)
        live_algo.trading_client = MagicMock(spec=LiveAlgorithmExecutor)
        live_algo.trading_client.current_data = Mock()
        live_algo.trading_client.current_data.current.return_value = 12

        live_algo.handle_data(data=sentinel.data)
        assert live_algo.broker.order.called
        assert live_algo.trading_client.current_data.current.called

    def test_data_portal_live_extends_ingested_data(self):
        assets = [self.asset_finder.retrieve_asset(1), ]
        rt_bars = pd.DataFrame(
            index=pd.date_range(start='2017-09-28 10:11:00',
                                end='2017-09-28 10:45:00',
                                freq='1 Min', tz='utc'),
            columns=pd.MultiIndex.from_product(
                [assets,
                 ['open', 'high', 'low', 'close', 'volume']]),
            data=np.random.randn(35, 5)
        )
        broker = MagicMock(Broker)
        broker.get_realtime_bars.return_value = rt_bars
        data_portal_live = DataPortalLive(
            broker,
            asset_finder=self.data_portal.asset_finder,
            trading_calendar=self.data_portal.trading_calendar,
            first_trading_day=self.data_portal._first_available_session,
            equity_daily_reader=(
                self.bcolz_equity_daily_bar_reader
                if self.DATA_PORTAL_USE_DAILY_DATA else
                None
            ),
            equity_minute_reader=(
                self.bcolz_equity_minute_bar_reader
                if self.DATA_PORTAL_USE_MINUTE_DATA else
                None
            ),
            adjustment_reader=(
                self.adjustment_reader
                if self.DATA_PORTAL_USE_ADJUSTMENTS else
                None
            ),
        )

        # Test with overall bar count > available realtime bar count
        end_dt = pd.to_datetime('2017-03-03 10:00:00', utc=True)
        bar_count = 1000
        combined_data = data_portal_live.get_history_window(
            assets, end_dt, bar_count=bar_count, frequency='1m',
            field='price', data_frequency='1m')

        expected_bars = rt_bars[-bar_count:].swaplevel(0, 1, axis=1)['close']
        assert len(combined_data) == bar_count
        assert expected_bars.isin(combined_data).all().all()

        # Test with overall bar count < available realtime bar count
        end_dt = pd.to_datetime('2017-03-03 10:00:00', utc=True)
        bar_count = 10
        combined_data = data_portal_live.get_history_window(
            assets, end_dt, bar_count=bar_count, frequency='1m',
            field='price', data_frequency='1m')

        expected_bars = rt_bars[-bar_count:].swaplevel(0, 1, axis=1)['close']
        assert len(combined_data) == bar_count
        assert expected_bars.isin(combined_data).all().all()


class TestIBBroker(WithSimParams, ZiplineTestCase):
    ASSET_FINDER_EQUITY_SIDS = (1, 2)
    ASSET_FINDER_EQUITY_SYMBOLS = ("SPY", "XIV")

    @staticmethod
    def _tws_bars():
        with patch('zipline.gens.brokers.ib_broker.TWSConnection.connect'):
            tws = TWSConnection("localhost:9999:1111")

        tws._add_bar('SPY', 12.4, 10,
                     pd.to_datetime('2017-09-27 10:30:00', utc=True),
                     10, 12.401, False)
        tws._add_bar('SPY', 12.41, 10,
                     pd.to_datetime('2017-09-27 10:30:40', utc=True),
                     20, 12.411, False)
        tws._add_bar('SPY', 12.44, 20,
                     pd.to_datetime('2017-09-27 10:31:10', utc=True),
                     40, 12.441, False)
        tws._add_bar('SPY', 12.74, 5,
                     pd.to_datetime('2017-09-27 10:37:10', utc=True),
                     45, 12.741, True)
        tws._add_bar('SPY', 12.99, 15,
                     pd.to_datetime('2017-09-27 12:10:00', utc=True),
                     60, 12.991, False)
        tws._add_bar('XIV', 100.4, 100,
                     pd.to_datetime('2017-09-27 9:32:00', utc=True),
                     100, 100.401, False)
        tws._add_bar('XIV', 100.41, 100,
                     pd.to_datetime('2017-09-27 9:32:20', utc=True),
                     200, 100.411, True)
        tws._add_bar('XIV', 100.44, 200,
                     pd.to_datetime('2017-09-27 9:41:10', utc=True),
                     400, 100.441, False)
        tws._add_bar('XIV', 100.74, 50,
                     pd.to_datetime('2017-09-27 11:42:10', utc=True),
                     450, 100.741, False)

        return tws.bars

    @staticmethod
    def _create_contract(symbol):
        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = 'STK'
        return contract

    @staticmethod
    def _create_order(action, qty, order_type, limit_price, stop_price):
        order = Order()
        order.m_action = action
        order.m_totalQuantity = qty
        order.m_auxPrice = stop_price
        order.m_lmtPrice = limit_price
        order.m_orderType = order_type
        return order

    @staticmethod
    def _create_order_state(status_):
        status = OrderState()
        status.m_status = status_
        return status

    @staticmethod
    def _create_exec_detail(order_id, shares, cum_qty, price, avg_price,
                            exec_time, exec_id):
        exec_detail = Execution()
        exec_detail.m_orderId = order_id
        exec_detail.m_shares = shares
        exec_detail.m_cumQty = cum_qty
        exec_detail.m_price = price
        exec_detail.m_avgPrice = avg_price
        exec_detail.m_time = exec_time
        exec_detail.m_execId = exec_id
        return exec_detail

    @patch('zipline.gens.brokers.ib_broker.TWSConnection')
    def test_get_spot_value(self, tws):
        dt = None  # dt is not used in real broker
        data_freq = 'minute'
        asset = self.env.asset_finder.retrieve_asset(1)
        bars = {'last_trade_price': [12, 10, 11, 14],
                'last_trade_size': [1, 2, 3, 4],
                'total_volume': [10, 10, 10, 10],
                'vwap': [12.1, 10.1, 11.1, 14.1],
                'single_trade_flag': [0, 1, 0, 1]}
        last_trade_times = [pd.to_datetime('2017-06-16 10:30:00', utc=True),
                            pd.to_datetime('2017-06-16 10:30:11', utc=True),
                            pd.to_datetime('2017-06-16 10:30:30', utc=True),
                            pd.to_datetime('2017-06-17 10:31:9', utc=True)]
        index = pd.DatetimeIndex(last_trade_times)
        broker = IBBroker(sentinel.tws_uri)
        tws.return_value.bars = {asset.symbol: pd.DataFrame(
            index=index, data=bars)}

        price = broker.get_spot_value(asset, 'price', dt, data_freq)
        last_trade = broker.get_spot_value(asset, 'last_traded', dt, data_freq)
        open_ = broker.get_spot_value(asset, 'open', dt, data_freq)
        high = broker.get_spot_value(asset, 'high', dt, data_freq)
        low = broker.get_spot_value(asset, 'low', dt, data_freq)
        close = broker.get_spot_value(asset, 'close', dt, data_freq)
        volume = broker.get_spot_value(asset, 'volume', dt, data_freq)

        # Only the last minute is taken into account, therefore
        # the first bar is ignored
        assert price == bars['last_trade_price'][-1]
        assert last_trade == last_trade_times[-1]
        assert open_ == bars['last_trade_price'][1]
        assert high == max(bars['last_trade_price'][1:])
        assert low == min(bars['last_trade_price'][1:])
        assert close == bars['last_trade_price'][-1]
        assert volume == sum(bars['last_trade_size'][1:])

    def test_get_realtime_bars_produces_correct_df(self):
        bars = self._tws_bars()

        with patch('zipline.gens.brokers.ib_broker.TWSConnection'):
            broker = IBBroker(sentinel.tws_uri)
            broker._tws.bars = bars

        assets = (self.env.asset_finder.retrieve_asset(1),
                  self.env.asset_finder.retrieve_asset(2))

        realtime_history = broker.get_realtime_bars(assets, '1m')

        asset_spy = self.env.asset_finder.retrieve_asset(1)
        asset_xiv = self.env.asset_finder.retrieve_asset(2)

        assert asset_spy in realtime_history
        assert asset_xiv in realtime_history

        spy = realtime_history[asset_spy]
        xiv = realtime_history[asset_xiv]

        assert list(spy.columns) == ['open', 'high', 'low', 'close', 'volume']
        assert list(xiv.columns) == ['open', 'high', 'low', 'close', 'volume']

        # There are 159 minutes between the first (XIV @ 2017-09-27 9:32:00)
        # and the last bar (SPY @ 2017-09-27 12:10:00)
        assert len(realtime_history) == 159

        spy_non_na = spy.dropna()
        xiv_non_na = xiv.dropna()
        assert len(spy_non_na) == 4
        assert len(xiv_non_na) == 3

        assert spy_non_na.iloc[0].name == pd.to_datetime(
            '2017-09-27 10:30:00', utc=True)
        assert spy_non_na.iloc[0].open == 12.40
        assert spy_non_na.iloc[0].high == 12.41
        assert spy_non_na.iloc[0].low == 12.40
        assert spy_non_na.iloc[0].close == 12.41
        assert spy_non_na.iloc[0].volume == 20

        assert spy_non_na.iloc[1].name == pd.to_datetime(
            '2017-09-27 10:31:00', utc=True)
        assert spy_non_na.iloc[1].open == 12.44
        assert spy_non_na.iloc[1].high == 12.44
        assert spy_non_na.iloc[1].low == 12.44
        assert spy_non_na.iloc[1].close == 12.44
        assert spy_non_na.iloc[1].volume == 20

        assert spy_non_na.iloc[-1].name == pd.to_datetime(
            '2017-09-27 12:10:00', utc=True)
        assert spy_non_na.iloc[-1].open == 12.99
        assert spy_non_na.iloc[-1].high == 12.99
        assert spy_non_na.iloc[-1].low == 12.99
        assert spy_non_na.iloc[-1].close == 12.99
        assert spy_non_na.iloc[-1].volume == 15

        assert xiv_non_na.iloc[0].name == pd.to_datetime(
            '2017-09-27 9:32:00', utc=True)
        assert xiv_non_na.iloc[0].open == 100.4
        assert xiv_non_na.iloc[0].high == 100.41
        assert xiv_non_na.iloc[0].low == 100.4
        assert xiv_non_na.iloc[0].close == 100.41
        assert xiv_non_na.iloc[0].volume == 200

    @patch('zipline.gens.brokers.ib_broker.symbol_lookup')
    def test_new_order_appears_in_orders(self, symbol_lookup):
        with patch('zipline.gens.brokers.ib_broker.TWSConnection.connect'):
            broker = IBBroker("localhost:9999:1111", account_id='TEST-123')
            broker._tws.nextValidId(0)

        asset = self.env.asset_finder.retrieve_asset(1)
        symbol_lookup.return_value = asset
        amount = -4
        limit_price = 43.1
        stop_price = 6
        style = StopLimitOrder(limit_price=limit_price, stop_price=stop_price)
        order = broker.order(asset, amount, style)

        assert len(broker.orders) == 1
        assert broker.orders[order.id] == order
        assert order.open
        assert order.asset == asset
        assert order.amount == amount
        assert order.limit == limit_price
        assert order.stop == stop_price
        assert (order.dt - pd.to_datetime('now', utc=True) <
                pd.Timedelta('10s'))

    @patch('zipline.gens.brokers.ib_broker.symbol_lookup')
    def test_orders_loaded_from_open_orders(self, symbol_lookup):
        with patch('zipline.gens.brokers.ib_broker.TWSConnection.connect'):
            broker = IBBroker("localhost:9999:1111", account_id='TEST-123')

        asset = self.env.asset_finder.retrieve_asset(1)
        symbol_lookup.return_value = asset

        ib_order_id = 3
        ib_contract = self._create_contract(str(asset.symbol))
        action, qty, order_type, limit_price, stop_price = \
            'SELL', 40, 'STP LMT', 4.3, 2
        ib_order = self._create_order(
            action, qty, order_type, limit_price, stop_price)
        ib_state = self._create_order_state('PreSubmitted')
        broker._tws.openOrder(ib_order_id, ib_contract, ib_order, ib_state)

        assert len(broker.orders) == 1
        zp_order = list(broker.orders.values())[-1]
        assert zp_order.broker_order_id == ib_order_id
        assert zp_order.status == ORDER_STATUS.HELD
        assert zp_order.open
        assert zp_order.asset == asset
        assert zp_order.amount == -40
        assert zp_order.limit == limit_price
        assert zp_order.stop == stop_price
        assert (zp_order.dt - pd.to_datetime('now', utc=True) <
                pd.Timedelta('10s'))

        @patch('zipline.gens.brokers.ib_broker.symbol_lookup')
        def test_orders_loaded_from_exec_details(self, symbol_lookup):
            with patch('zipline.gens.brokers.ib_broker.TWSConnection.connect'):
                broker = IBBroker("localhost:9999:1111", account_id='TEST-123')

            asset = self.env.asset_finder.retrieve_asset(1)
            symbol_lookup.return_value = asset

            (req_id, ib_order_id, shares, cum_qty,
             price, avg_price, exec_time, exec_id) = (7, 3, 12, 40,
                                                      12.43, 12.50,
                                                      '20160101 14:20', 4)
            ib_contract = self._create_contract(str(asset.symbol))
            exec_detail = self._create_exec_detail(
                ib_order_id, shares, cum_qty, price, avg_price,
                exec_time, exec_id)
            broker._tws.execDetails(req_id, ib_contract, exec_detail)

            assert len(broker.orders) == 1
            zp_order = list(broker.orders.values())[-1]
            assert zp_order.broker_order_id == ib_order_id
            assert zp_order.open
            assert zp_order.asset == asset
            assert zp_order.amount == -40
            assert zp_order.limit == limit_price
            assert zp_order.stop == stop_price
            assert (zp_order.dt - pd.to_datetime('now', utc=True) <
                    pd.Timedelta('10s'))

    @patch('zipline.gens.brokers.ib_broker.symbol_lookup')
    def test_orders_updated_from_order_status(self, symbol_lookup):
        with patch('zipline.gens.brokers.ib_broker.TWSConnection.connect'):
            broker = IBBroker("localhost:9999:1111", account_id='TEST-123')
            broker._tws.nextValidId(0)

        # orderStatus calls only work if a respective order has been created
        asset = self.env.asset_finder.retrieve_asset(1)
        symbol_lookup.return_value = asset
        amount = -4
        limit_price = 43.1
        stop_price = 6
        style = StopLimitOrder(limit_price=limit_price, stop_price=stop_price)
        order = broker.order(asset, amount, style)

        ib_order_id = order.broker_order_id
        status = 'Filled'
        filled = 14
        remaining = 9
        avg_fill_price = 12.4
        perm_id = 99
        parent_id = 88
        last_fill_price = 12.3
        client_id = 1111
        why_held = ''

        broker._tws.orderStatus(ib_order_id,
                                status, filled, remaining, avg_fill_price,
                                perm_id, parent_id, last_fill_price, client_id,
                                why_held)

        assert len(broker.orders) == 1
        zp_order = list(broker.orders.values())[-1]
        assert zp_order.broker_order_id == ib_order_id
        assert zp_order.status == ORDER_STATUS.FILLED
        assert not zp_order.open
        assert zp_order.asset == asset
        assert zp_order.amount == amount
        assert zp_order.limit == limit_price
        assert zp_order.stop == stop_price
        assert (zp_order.dt - pd.to_datetime('now', utc=True) <
                pd.Timedelta('10s'))

    @patch('zipline.gens.brokers.ib_broker.symbol_lookup')
    def test_multiple_orders(self, symbol_lookup):
        with patch('zipline.gens.brokers.ib_broker.TWSConnection.connect'):
            broker = IBBroker("localhost:9999:1111", account_id='TEST-123')
            broker._tws.nextValidId(0)

        asset = self.env.asset_finder.retrieve_asset(1)
        symbol_lookup.return_value = asset

        order_count = 0
        for amount, order_style in [
                (-112, StopLimitOrder(limit_price=9, stop_price=1)),
                (43, LimitOrder(limit_price=10)),
                (-99, StopOrder(stop_price=8)),
                (-32, MarketOrder())]:
            order = broker.order(asset, amount, order_style)
            order_count += 1

            assert order_count == len(broker.orders)
            assert broker.orders[order.id] == order
            is_buy = amount > 0
            assert order.stop == order_style.get_stop_price(is_buy)
            assert order.limit == order_style.get_limit_price(is_buy)

    def test_order_ref_serdes(self):
        # Even though _creater_order_ref and _parse_order_ref is private
        # it is helpful to test as it plays a key role to re-create orders
        order = self._create_order("BUY", 66, "STP LMT", 13.4, 44.2)
        serialized = IBBroker._create_order_ref(order)
        deserialized = IBBroker._parse_order_ref(serialized)
        assert deserialized['action'] == order.m_action
        assert deserialized['qty'] == order.m_totalQuantity
        assert deserialized['order_type'] == order.m_orderType
        assert deserialized['limit_price'] == order.m_lmtPrice
        assert deserialized['stop_price'] == order.m_auxPrice
        assert (deserialized['dt'] - pd.to_datetime('now', utc=True) <
                pd.Timedelta('10s'))

    @patch('zipline.gens.brokers.ib_broker.symbol_lookup')
    def test_transactions_not_created_for_incompl_orders(self, symbol_lookup):
        with patch('zipline.gens.brokers.ib_broker.TWSConnection.connect'):
            broker = IBBroker("localhost:9999:1111", account_id='TEST-123')
            broker._tws.nextValidId(0)

        asset = self.env.asset_finder.retrieve_asset(1)
        symbol_lookup.return_value = asset
        amount = -4
        limit_price = 43.1
        stop_price = 6
        style = StopLimitOrder(limit_price=limit_price, stop_price=stop_price)
        order = broker.order(asset, amount, style)
        assert not broker.transactions
        assert len(broker.orders) == 1
        assert broker.orders[order.id].open

        ib_order_id = order.broker_order_id
        ib_contract = self._create_contract(str(asset.symbol))
        action, qty, order_type, limit_price, stop_price = \
            'SELL', 4, 'STP LMT', 4.3, 2
        ib_order = self._create_order(
            action, qty, order_type, limit_price, stop_price)
        ib_state = self._create_order_state('PreSubmitted')
        broker._tws.openOrder(ib_order_id, ib_contract, ib_order, ib_state)

        broker._tws.orderStatus(ib_order_id, status='Cancelled', filled=0,
                                remaining=4, avg_fill_price=0.0, perm_id=4,
                                parent_id=4, last_fill_price=0.0, client_id=32,
                                why_held='')
        assert not broker.transactions
        assert len(broker.orders) == 1
        assert not broker.orders[order.id].open

        broker._tws.orderStatus(ib_order_id, status='Inactive', filled=0,
                                remaining=4, avg_fill_price=0.0, perm_id=4,
                                parent_id=4, last_fill_price=0.0,
                                client_id=1111, why_held='')
        assert not broker.transactions
        assert len(broker.orders) == 1
        assert not broker.orders[order.id].open

    @patch('zipline.gens.brokers.ib_broker.symbol_lookup')
    def test_transactions_created_for_complete_orders(self, symbol_lookup):
        with patch('zipline.gens.brokers.ib_broker.TWSConnection.connect'):
            broker = IBBroker("localhost:9999:1111", account_id='TEST-123')
            broker._tws.nextValidId(0)

        asset = self.env.asset_finder.retrieve_asset(1)
        symbol_lookup.return_value = asset

        order_count = 0
        for amount, order_style in [
                (-112, StopLimitOrder(limit_price=9, stop_price=1)),
                (43, LimitOrder(limit_price=10)),
                (-99, StopOrder(stop_price=8)),
                (-32, MarketOrder())]:
            order = broker.order(asset, amount, order_style)
            broker._tws.orderStatus(order.broker_order_id, 'Filled',
                                    filled=int(fabs(amount)), remaining=0,
                                    avg_fill_price=111, perm_id=0, parent_id=1,
                                    last_fill_price=112, client_id=1111,
                                    why_held='')
            contract = self._create_contract(str(asset.symbol))
            (shares, cum_qty, price, avg_price, exec_time, exec_id) = \
                (int(fabs(amount)), int(fabs(amount)), 12.3, 12.31,
                 pd.to_datetime('now', utc=True), order_count)
            exec_detail = self._create_exec_detail(
                order.broker_order_id, shares, cum_qty,
                price, avg_price, exec_time, exec_id)
            broker._tws.execDetails(0, contract, exec_detail)
            order_count += 1

            assert len(broker.transactions) == order_count
            transactions = [tx
                            for tx in broker.transactions.values()
                            if tx.order_id == order.id]
            assert len(transactions) == 1

            assert broker.transactions[exec_id].asset == asset
            assert broker.transactions[exec_id].amount == order.amount
            assert (broker.transactions[exec_id].dt -
                    pd.to_datetime('now', utc=True) < pd.Timedelta('10s'))
            assert broker.transactions[exec_id].price == price
            assert broker.transactions[exec_id].commission == 0


class TestALPACABroker(WithSimParams, ZiplineTestCase):
    ASSET_FINDER_EQUITY_SIDS = (1, 2)
    ASSET_FINDER_EQUITY_SYMBOLS = ("SPY", "XIV")

    @patch('zipline.gens.brokers.alpaca_broker.tradeapi')
    def test_get_realtime_bars(self, tradeapi):
        api = tradeapi.REST()
        bars = [
            {
                'time': '2017-06-17T10:31:00-0400',
                'open': 102.0,
                'high': 102.5,
                'low': 101.5,
                'close': 102.1,
                'volume': 998,
            },
            {
                'time': '2017-06-17T10:32:00-0400',
                'open': 103.0,
                'high': 103.5,
                'low': 102.5,
                'close': 103.1,
                'volume': 996,
            },
        ]
        api.list_bars.return_value = [
            apca.AssetBars({
                'symbol': 'SPY',
                'bars': bars,
            })
        ]
        broker = ALPACABroker('')
        asset = self.env.asset_finder.retrieve_asset(1)
        ret = broker.get_realtime_bars(asset, '1m')
        assert ret[asset, 'open'].values[0] == 102.0

        ret = broker.get_realtime_bars([asset], '1m')
        assert ret[asset, 'close'].values[1] == 103.1

    @patch('zipline.gens.brokers.alpaca_broker.tradeapi')
    def test_get_spot_value(self, tradeapi):
        api = tradeapi.REST()

        dt = None  # dt is not used in real broker
        data_freq = 'minute'
        asset = self.env.asset_finder.retrieve_asset(1)
        bar = {'time': '2017-06-17T10:31:09-0400',
               'open': 103.0,
               'high': 103.5,
               'low': 102.5,
               'close': 103.1,
               'volume': 996}
        broker = ALPACABroker('')
        api.list_bars.return_value = [
            apca.AssetBars({
                'symbol': 'SPY',
                'bars': [bar],
            })
        ]

        quote = {
            'last': 103.8,
            'last_timestamp': '2017-06-17T10:31:13-0400',
        }
        api.list_quotes.return_value = [
            apca.Quote(quote)
        ]

        price = broker.get_spot_value(asset, 'price', dt, data_freq)
        last_trade = broker.get_spot_value(asset, 'last_traded', dt, data_freq)
        open_ = broker.get_spot_value(asset, 'open', dt, data_freq)
        high = broker.get_spot_value(asset, 'high', dt, data_freq)
        low = broker.get_spot_value(asset, 'low', dt, data_freq)
        close = broker.get_spot_value(asset, 'close', dt, data_freq)
        volume = broker.get_spot_value(asset, 'volume', dt, data_freq)

        assert price == quote['last']
        assert last_trade == pd.Timestamp(quote['last_timestamp'])
        assert open_ == bar['open']
        assert high == bar['high']
        assert low == bar['low']
        assert close == bar['close']
        assert volume == bar['volume']

        assets = [asset]
        price = broker.get_spot_value(assets, 'price', dt, data_freq)[0]
        last_trade = broker.get_spot_value(
            assets, 'last_traded', dt, data_freq)[0]
        open_ = broker.get_spot_value(assets, 'open', dt, data_freq)[0]
        high = broker.get_spot_value(assets, 'high', dt, data_freq)[0]
        low = broker.get_spot_value(assets, 'low', dt, data_freq)[0]
        close = broker.get_spot_value(assets, 'close', dt, data_freq)[0]
        volume = broker.get_spot_value(assets, 'volume', dt, data_freq)[0]

        assert price == quote['last']
        assert last_trade == pd.Timestamp(quote['last_timestamp'])
        assert open_ == bar['open']
        assert high == bar['high']
        assert low == bar['low']
        assert close == bar['close']
        assert volume == bar['volume']

    @patch('zipline.gens.brokers.alpaca_broker.tradeapi')
    def test_is_alive(self, tradeapi):
        api = tradeapi.REST()
        broker = ALPACABroker('')
        assert broker.is_alive()
        api.get_account.side_effect = Exception()
        assert not broker.is_alive()

    @patch('zipline.gens.brokers.alpaca_broker.symbol_lookup')
    @patch('zipline.gens.brokers.alpaca_broker.tradeapi')
    def test_order(self, tradeapi, symbol_lookup):
        api = tradeapi.REST()
        asset = self.env.asset_finder.retrieve_asset(1)
        symbol_lookup.return_value = asset
        broker = ALPACABroker('')
        amount = 10

        submitted_orders = []

        def submit_order(symbol, qty, side, type, time_in_force,
                         limit_price, stop_price, client_order_id):
            o = apca.Order({
                'symbol': symbol,
                'qty': str(qty),
                'side': side,
                'type': type,
                'time_in_force': time_in_force,
                'limit_price': limit_price,
                'stop_price': stop_price,
                'client_order_id': client_order_id,
                'submitted_at': '2017-06-01T10:30:00-0400',
                'filled_at': None,
                'filled_qty': None,
                'canceled_at': None,
                'failed_at': None,
            })
            submitted_orders.append(o)
            return o
        api.submit_order = submit_order
        order = broker.order(asset, amount, MarketOrder())
        assert order.limit is None
        assert submitted_orders[-1].side == 'buy'
        assert submitted_orders[-1].type == 'market'

        order = broker.order(asset, -amount, LimitOrder(210.00))
        assert order.limit == 210.00
        assert order.amount == -amount
        assert submitted_orders[-1].side == 'sell'
        assert submitted_orders[-1].type == 'limit'
        assert submitted_orders[-1].limit_price is not None

        order = broker.order(asset, amount, StopOrder(211))
        assert order.stop == 211.00
        assert submitted_orders[-1].side == 'buy'
        assert submitted_orders[-1].type == 'stop'
        assert submitted_orders[-1].stop_price is not None

        order = broker.order(asset, -amount, StopLimitOrder(210, 211))
        assert order.limit == 210.00
        assert order.stop == 211.00
        assert submitted_orders[-1].side == 'sell'
        assert submitted_orders[-1].type == 'stop_limit'
        assert submitted_orders[-1].limit_price is not None
        assert submitted_orders[-1].stop_price is not None

        api.get_order_by_client_order_id.return_value = submitted_orders[-1]

        def cancel_order(self, order_id):
            assert order.id == order_id

        broker.cancel_order(order.id)

    @patch('zipline.gens.brokers.alpaca_broker.symbol_lookup')
    @patch('zipline.gens.brokers.alpaca_broker.tradeapi')
    def test_orders(self, tradeapi, symbol_lookup):
        asset = self.env.asset_finder.retrieve_asset(1)
        symbol_lookup.return_value = asset
        api = tradeapi.REST()
        id1 = '98486056-5b88-48be-a64c-342e8b751cb2'
        order1 = apca.Order({
            'id': 'order1',
            'symbol': 'SPY',
            'qty': '10',
            'side': 'buy',
            'filled_at': '2017-06-01T10:05:00-0400',
            'submitted_at': '2017-06-01T10:04:30-0400',
            'filled_qty': '10',
            'filled_avg_price': '210.05',
            'failed_at': None,
            'canceled_at': None,
            'limit_price': '210.32',
            'stop_price': '210.00',
            'client_order_id': id1,
        })
        # OPEN
        id2 = 'debb01c4-e40e-4ac0-a6b0-8aed1a7fc126'
        order2 = apca.Order({
            'id': 'order2',
            'symbol': 'SPY',
            'qty': '10',
            'side': 'sell',
            'filled_at': None,
            'submitted_at': '2017-06-01T10:04:30-0400',
            'filled_qty': None,
            'filled_avg_price': None,
            'failed_at': None,
            'canceled_at': None,
            'limit_price': '210.32',
            'stop_price': '210.00',
            'client_order_id': id2,
        })
        # CANCELED
        id3 = 'ac5c7cda-f8ff-4b6f-ad17-fd68f7769ae9'
        order3 = apca.Order({
            'id': 'order3',
            'symbol': 'SPY',
            'qty': '10',
            'side': 'sell',
            'filled_at': None,
            'submitted_at': '2017-06-01T10:04:30-0400',
            'filled_qty': None,
            'filled_avg_price': None,
            'failed_at': None,
            'canceled_at': '2017-06-01T10:04:31-0400',
            'limit_price': '210.32',
            'stop_price': '210.00',
            'client_order_id': id3,
        })
        # REJECTED
        id4 = 'a4018fb1-cc9c-429a-a452-f92e8dc4096b'
        order4 = apca.Order({
            'id': 'order4',
            'symbol': 'SPY',
            'qty': '10',
            'side': 'sell',
            'filled_at': None,
            'submitted_at': '2017-06-01T10:04:30-0400',
            'filled_qty': None,
            'filled_avg_price': None,
            'failed_at': '2017-06-01T10:04:31-0400',
            'canceled_at': None,
            'limit_price': '210.32',
            'stop_price': '210.00',
            'client_order_id': id4,
        })
        api.list_orders.return_value = [
            order1,
            order2,
            order3,
            order4,
        ]
        broker = ALPACABroker('')
        orders = broker.orders
        assert orders[id1].status == ORDER_STATUS.FILLED
        assert orders[id1].filled == int(order1.filled_qty)
        assert orders[id1].amount == int(order1.qty)
        assert orders[id1].asset == asset
        assert orders[id2].status == ORDER_STATUS.OPEN
        assert orders[id3].status == ORDER_STATUS.CANCELLED
        assert orders[id4].status == ORDER_STATUS.REJECTED

        trans = broker.orders
        assert len(trans) == 4

        trans = broker.transactions
        assert len(trans) == 1
        assert trans[id1].amount == 10

    @patch('zipline.gens.brokers.alpaca_broker.symbol_lookup')
    @patch('zipline.gens.brokers.alpaca_broker.tradeapi')
    def test_portfolio(self, tradeapi, symbol_lookup):
        api = tradeapi.REST()
        asset = self.env.asset_finder.retrieve_asset(1)
        ret_account = apca.Account({
            'cash': '5000.00',
            'portfolio_value': '7000.00'
        })
        api.get_account.return_value = ret_account
        ret_positions = [
            apca.Position({
                'symbol': 'SPY',
                'qty': '10',
                'cost_basis': '210.00',
            })
        ]
        api.list_positions.return_value = ret_positions
        ret_quotes = [
            apca.Quote({
                'symbol': 'SPY',
                'last': 210.05,
                'last_timestamp': '2017-06-01T10:03:03-0400',
            })
        ]
        api.list_quotes.return_value = ret_quotes
        symbol_lookup.return_value = asset
        broker = ALPACABroker('')
        portfolio = broker.portfolio

        assert portfolio.cash == float(ret_account.cash)

        account = broker.account
        assert account.buying_power == float(ret_account.cash)
        assert account.total_position_value == float(
            ret_account.portfolio_value) - float(ret_account.cash)

        positions = portfolio.positions
        assert positions[asset].cost_basis == float(
            ret_positions[0].cost_basis)
        assert positions[asset].last_sale_price == float(ret_quotes[0].last)

        portfolio = broker.portfolio

    @patch('zipline.gens.brokers.alpaca_broker.tradeapi')
    def test_last_trade_dt(self, tradeapi):
        asset = self.env.asset_finder.retrieve_asset(1)
        api = tradeapi.REST()
        broker = ALPACABroker('')

        def get_quote(symbol):
            assert symbol == 'SPY'
            return apca.Quote({
                'symbol': 'SPY',
                'last': 210,
                'last_timestamp': '2016-06-01T10:27:00-0400',
            })
        api.get_quote = get_quote
        ret = broker.get_last_traded_dt(asset)
        assert ret.minute == 27

    @patch('zipline.gens.brokers.alpaca_broker.tradeapi')
    def test_misc(self, tradeapi):
        broker = ALPACABroker('')
        assert broker.subscribe_to_market_data(None) is None
        assert broker.subscribed_assets() == []
        assert broker.time_skew == pd.Timedelta('0sec')


class TestBlotterLive(WithTradingEnvironment, ZiplineTestCase):
    ASSET_FINDER_EQUITY_SIDS = (1, 2)
    ASSET_FINDER_EQUITY_SYMBOLS = ("SPY", "XIV")

    @staticmethod
    def _get_orders(asset1, asset2):
        return {
            sentinel.order_id1: ZPOrder(
                dt=sentinel.dt, asset=asset1, amount=12,
                stop=sentinel.stop1, limit=sentinel.limit1,
                id=sentinel.order_id1),
            sentinel.order_id2: ZPOrder(
                dt=sentinel.dt, asset=asset1, amount=-12,
                limit=sentinel.limit2, id=sentinel.order_id2),
            sentinel.order_id3: ZPOrder(
                dt=sentinel.dt, asset=asset2, amount=3,
                stop=sentinel.stop2, limit=sentinel.limit2,
                id=sentinel.order_id3),
            sentinel.order_id4: ZPOrder(
                dt=sentinel.dt, asset=asset2, amount=-122,
                id=sentinel.order_id4),
        }

    @staticmethod
    def _get_execution(price, qty, dt):
        execution = Execution()
        execution.m_price = price
        execution.m_cumQty = qty
        execution.m_time = dt
        return execution

    def test_open_orders(self):
        broker = MagicMock(Broker)
        blotter = BlotterLive(data_frequency='minute', broker=broker)
        assert not blotter.open_orders

        asset1 = self.env.asset_finder.retrieve_asset(1)
        asset2 = self.env.asset_finder.retrieve_asset(2)

        all_orders = self._get_orders(asset1, asset2)
        all_orders[sentinel.order_id4].filled = -122
        broker.orders = all_orders

        assert len(blotter.open_orders) == 2

        assert len(blotter.open_orders[asset1]) == 2
        assert all_orders[sentinel.order_id1] in blotter.open_orders[asset1]
        assert all_orders[sentinel.order_id2] in blotter.open_orders[asset1]

        assert len(blotter.open_orders[asset2]) == 1
        assert blotter.open_orders[asset2][0].id == sentinel.order_id3

    def test_get_transactions(self):
        broker = MagicMock(Broker)
        blotter = BlotterLive(data_frequency='minute', broker=broker)

        asset1 = self.env.asset_finder.retrieve_asset(1)
        asset2 = self.env.asset_finder.retrieve_asset(2)

        broker.orders = {}
        broker.transactions = {}
        new_transactions, new_commissions, new_closed_orders = \
            blotter.get_transactions(None)
        assert not new_transactions
        assert not new_commissions
        assert not new_closed_orders

        broker.orders = self._get_orders(asset1, asset2)
        new_transactions, new_commissions, new_closed_orders = \
            blotter.get_transactions(None)
        assert not new_transactions
        assert not new_commissions
        assert not new_closed_orders

        broker.orders[sentinel.order_id4].filled = \
            broker.orders[sentinel.order_id4].amount
        broker.transactions['exec_4'] = \
            Transaction(asset=asset2,
                        amount=broker.orders[sentinel.order_id4].amount,
                        dt=pd.to_datetime('now', utc=True),
                        price=123, order_id=sentinel.order_id4,
                        commission=12)
        new_transactions, new_commissions, new_closed_orders = \
            blotter.get_transactions(None)
        assert new_closed_orders == [broker.orders[sentinel.order_id4], ]
        assert new_commissions == [{
            'asset': asset2,
            'cost': 12,
            'order': broker.orders[sentinel.order_id4]
        }]
        assert new_transactions == [list(broker.transactions.values())[0], ]

        new_transactions, new_commissions, new_closed_orders = \
            blotter.get_transactions(None)
        assert not new_transactions
        assert not new_commissions
        assert not new_closed_orders

        broker.orders[sentinel.order_id3].filled = \
            broker.orders[sentinel.order_id3].amount
        broker.transactions['exec_3'] = \
            Transaction(asset=asset1,
                        amount=broker.orders[sentinel.order_id3].amount,
                        dt=pd.to_datetime('now', utc=True),
                        price=1234, order_id=sentinel.order_id3,
                        commission=1)

        broker.orders[sentinel.order_id2].filled = \
            broker.orders[sentinel.order_id2].amount
        broker.transactions['exec_2'] = \
            Transaction(asset=asset2,
                        amount=broker.orders[sentinel.order_id2].amount,
                        dt=pd.to_datetime('now', utc=True),
                        price=12.34, order_id=sentinel.order_id2,
                        commission=0)

        new_transactions, new_commissions, new_closed_orders = \
            blotter.get_transactions(None)
        assert len(new_closed_orders) == 2
        assert broker.orders[sentinel.order_id3] in new_closed_orders
        assert broker.orders[sentinel.order_id2] in new_closed_orders

        assert len(new_commissions) == 2
        assert {'asset': asset2,
                'cost': 0,
                'order': broker.orders[sentinel.order_id2]}\
            in new_commissions
        assert {'asset': asset1,
                'cost': 1,
                'order': broker.orders[sentinel.order_id3]} \
            in new_commissions
        assert len(new_transactions) == 2
        assert broker.transactions['exec_2'] in new_transactions
        assert broker.transactions['exec_3'] in new_transactions

        new_transactions, new_commissions, new_closed_orders = \
            blotter.get_transactions(None)
        assert not new_transactions
        assert not new_commissions
        assert not new_closed_orders
