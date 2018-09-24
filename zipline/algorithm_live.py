#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from datetime import time
import os.path
from datetime import time, timedelta
from locale import format

import logbook
import pandas as pd

import trading_calendars as cal
from IPython import embed
from trading_calendars import get_calendar
from trading_calendars.utils.pandas_utils import days_at_time
from zipline.finance.blotter.blotter_live import BlotterLive
from zipline.algorithm import TradingAlgorithm
from zipline.errors import ScheduleFunctionOutsideTradingStart
from zipline.gens.realtimeclock import RealtimeClock
from zipline.gens.tradesimulation import AlgorithmSimulator
from zipline.utils.api_support import ZiplineAPI, \
    allowed_only_in_before_trading_start, api_method
from zipline.utils.pandas_utils import normalize_date
from zipline.utils.serialization_utils import load_context, store_context
from zipline.finance.metrics import MetricsTracker, load as load_metrics_set

log = logbook.Logger("Live Trading")
# how many minutes before Trading starts needs the function before_trading_starts
# be launched
_minutes_before_trading_starts = 345

class LiveAlgorithmExecutor(AlgorithmSimulator):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)

    def _cleanup_expired_assets(self, dt, position_assets):
        # In simulation this is used to close assets in simulation end date, which
        # makes a lot of sense in our case, "simulation end" is set to 1 day from
        # now (we might want to fix that in the future too) BUT, in live mode we don't
        # have a simulation end date, and we should let the algorithm decide when to
        # close the assets.
        pass

class LiveTradingAlgorithm(TradingAlgorithm):
    def __init__(self, *args, **kwargs):
        self.broker = kwargs.pop('broker', None)
        self.orders = {}

        self.algo_filename = kwargs.get('algo_filename', "<algorithm>")
        self.state_filename = kwargs.pop('state_filename', None)
        self.realtime_bar_target = kwargs.pop('realtime_bar_target', None)
        # Persistence blacklist/whitelists and excludes gives a way to include
        # exclude (and not persists if inititaed) or excluded from the seria-
        # lization function that reinstate or save the context variable 
        # to its last state.
        # trading client can never be serialized, the initialized function and
        # perf tracker remember the context variables and the past parformance
        # and need to be whitelisted
        self._context_persistence_blacklist = ['trading_client']
        self._context_persistence_whitelist = ['initialized', 'perf_tracker']
        self._context_persistence_excludes = []
        
        #Blotter always arrives here with simulation blotter, overwriting this
        #with BlotterLive
        blotter_live = BlotterLive(
            data_frequency=kwargs['sim_params'].data_frequency,
            broker=self.broker)
        kwargs['blotter'] = blotter_live

        super(self.__class__, self).__init__(*args, **kwargs)
        self.asset_finder.is_live = True
        log.info("initialization done")

    def initialize(self, *args, **kwargs):

        self._context_persistence_excludes = \
            self._context_persistence_blacklist + \
            [e for e in self.__dict__.keys()
             if e not in self._context_persistence_whitelist]

        if os.path.isfile(self.state_filename):
            log.info("Loading state from {}".format(self.state_filename))
            load_context(self.state_filename,
                         context=self,
                         checksum=self.algo_filename)
            return

        with ZiplineAPI(self):
            super(self.__class__, self).initialize(*args, **kwargs)
            store_context(self.state_filename,
                          context=self,
                          checksum=self.algo_filename,
                          exclude_list=self._context_persistence_excludes)

    def handle_data(self, data):
        super(self.__class__, self).handle_data(data)
        store_context(self.state_filename,
                      context=self,
                      checksum=self.algo_filename,
                      exclude_list=self._context_persistence_excludes)

    def _create_clock(self):
        # This method is taken from TradingAlgorithm.
        # The clock has been replaced to use RealtimeClock
        trading_o_and_c = self.trading_calendar.schedule.ix[
            self.sim_params.sessions]
        assert self.sim_params.emission_rate == 'minute'

        minutely_emission = True
        market_opens = trading_o_and_c['market_open']
        market_closes = trading_o_and_c['market_close']

        # The calendar's execution times are the minutes over which we actually
        # want to run the clock. Typically the execution times simply adhere to
        # the market open and close times. In the case of the futures calendar,
        # for example, we only want to simulate over a subset of the full 24
        # hour calendar, so the execution times dictate a market open time of
        # 6:31am US/Eastern and a close of 5:00pm US/Eastern.
        execution_opens = \
            self.trading_calendar.execution_time_from_open(market_opens)
        execution_closes = \
            self.trading_calendar.execution_time_from_close(market_closes)

        before_trading_start_minutes = ((pd.to_datetime(execution_opens.values)
                            .tz_localize('UTC')
                            .tz_convert('US/Eastern')+
                            timedelta(minutes=-_minutes_before_trading_starts))
                            .tz_convert('UTC')
        )

        return RealtimeClock(
            self.sim_params.sessions,
            execution_opens,
            execution_closes,
            before_trading_start_minutes,
            minute_emission=minutely_emission,
            time_skew=self.broker.time_skew,
            is_broker_alive=self.broker.is_alive,
            stop_execution_callback=self._stop_execution_callback
        )

    def _create_generator(self, sim_params):
        # Call the simulation trading algorithm for side-effects:
        # it creates the perf tracker
        TradingAlgorithm._create_generator(self, self.sim_params)

        #self.metrics_tracker is available now, but we will create a new object
        #so we can influence the capital_base. TODO: broker object needs to be
        #able to give account value without trying to set the account object
        #Now solved with creating new function in broker: get_account_from_broker
        self.metrics_tracker = metrics_tracker = self._create_metrics_tracker()
        benchmark_source = self._create_benchmark_source()
        metrics_tracker.handle_start_of_simulation(benchmark_source)

        #attach metrics_tracker to broker
        self.broker.set_metrics_tracker(self.metrics_tracker)

        self.trading_client = LiveAlgorithmExecutor(
            self,
            sim_params,
            self.data_portal,
            self.trading_client.clock,
            self._create_benchmark_source(),
            self.restrictions,
            universe_func=self._calculate_universe
        )

        return self.trading_client.transform()

    def updated_portfolio(self):
        return self.broker.portfolio

    def updated_account(self):
        return self.broker.account

    @api_method
    @allowed_only_in_before_trading_start(
        ScheduleFunctionOutsideTradingStart())
    def schedule_function(self,
                          func,
                          date_rule=None,
                          time_rule=None,
                          half_days=True,
                          calendar=None):
        # If the scheduled_function() is called from initalize()
        # then the state persistence would need to take care of storing and
        # restoring the scheduled functions too (as initialize() only called
        # once in the algorithm's life). Persisting scheduled functions are
        # difficult as they are not serializable by default.
        # We enforce scheduled functions to be called only from
        # before_trading_start() in live trading with a decorator.
        super(self.__class__, self).schedule_function(func,
                                                      date_rule,
                                                      time_rule,
                                                      half_days,
                                                      calendar)

    @api_method
    def symbol(self, symbol_str):
        # This method works around the problem of not being able to trade
        # assets which does not have ingested data for the day of trade.
        # Normally historical data is loaded to bundle and the asset's
        # end_date and auto_close_date is set based on the last entry from
        # the bundle db. LiveTradingAlgorithm does not override order_value(),
        # order_percent() & order_target(). Those higher level ordering
        # functions provide a safety net to not to trade de-listed assets.
        # If the asset is returned as it was ingested (end_date=yesterday)
        # then CannotOrderDelistedAsset exception will be raised from the
        # higher level order functions.
        #
        # Hence, we are increasing the asset's end_date by 10,000 days.
        # The ample buffer is provided for two reasons:
        # 1) assets are often stored in algo's context through initialize(),
        #    which is called once and persisted at live trading. 10,000 days
        #    enables 27+ years of trading, which is more than enough.
        # 2) Tool - 10,000 Days is brilliant!

        asset = super(self.__class__, self).symbol(symbol_str)
        tradeable_asset = asset.to_dict()
        tradeable_asset['end_date'] = pd.Timestamp(ts_input='2027-01-01', tz='UTC')

        tradeable_asset['auto_close_date'] = None
        
        return asset.from_dict(tradeable_asset)

    def run(self, *args, **kwargs):
        daily_stats = super(self.__class__, self).run(*args, **kwargs)
        self.on_exit()
        return daily_stats

    def on_exit(self):
        if not self.realtime_bar_target:
            return

        log.info("Storing realtime bars to: {}".format(
            self.realtime_bar_target))

        today = str(pd.to_datetime('today').date())
        subscribed_assets = self.broker.subscribed_assets
        realtime_history = self.broker.get_realtime_bars(subscribed_assets,
                                                         '1m')

        if not os.path.exists(self.realtime_bar_target):
            os.mkdir(self.realtime_bar_target)

        for asset in subscribed_assets:
            filename = "ZL-%s-%s.csv" % (asset.symbol, today)
            path = os.path.join(self.realtime_bar_target, filename)
            realtime_history[asset].to_csv(path, mode='a',
                                           index_label='datetime',
                                           header=not os.path.exists(path))

    def _pipeline_output(self, pipeline, chunks, name):
        # This method is taken from TradingAlgorithm.
        """
        Internal implementation of `pipeline_output`.

        For Live Algo's we have to get the previous session as the Pipeline wont work without,
        it will extrapolate such that it tries to get data for get_datetime which
        is today

        """
        today = normalize_date(self.get_datetime())
        prev_session = normalize_date(self.trading_calendar.previous_open(today))

        log.info('today in _pipeline_output : {}'.format(prev_session))

        try:
            data = self._pipeline_cache.get(name, prev_session)
        except KeyError:
            # Calculate the next block.
            data, valid_until = self.run_pipeline(
                pipeline, prev_session, next(chunks),
            )
            self._pipeline_cache.set(name, data, valid_until)

        # Now that we have a cached result, try to return the data for today.
        try:
            return data.loc[prev_session]
        except KeyError:
            # This happens if no assets passed the pipeline screen on a given
            # day.
            return pd.DataFrame(index=[], columns=data.columns)
