from unittest import TestCase
import pandas as pd
from datetime import time
from collections import defaultdict

from mock import patch

from zipline.gens.livetrading import (
    RealtimeClock,
    SESSION_START,
    BEFORE_TRADING_START_BAR,
    BAR,
    SESSION_END
)

from zipline.utils.calendars import get_calendar
from zipline.utils.calendars.trading_calendar import days_at_time


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

    def test_all_events_emitted(self):
        with patch('zipline.gens.livetrading.pd.to_datetime') as to_dt, \
             patch('zipline.gens.livetrading.sleep') as sleep:
            clock = iter(RealtimeClock(
                pd.Timedelta("0s"),
                self.sessions,
                self.opens,
                self.closes,
                days_at_time(self.sessions, time(8, 45), "US/Eastern"),
                True
            ))

            global current_clock
            current_clock = pd.Timestamp("2017-04-20 00:00", tz='UTC')

            def advance_clock(*args, **kwargs):
                global current_clock
                current_clock += pd.Timedelta('1M')

            def get_clock(*args, **kwargs):
                global current_clock
                return current_clock

            to_dt.side_effect = get_clock
            sleep.side_effect = advance_clock
            self.assertEqual(next(clock), (pd.Timestamp("2017-04-20 00:00", tz='UTC'), SESSION_START))

            self.assertEqual(next(clock), (pd.Timestamp("2017-04-20 12:45", tz='UTC'), BEFORE_TRADING_START_BAR))
            self.assertTrue(sleep.called)
            advance_clock()  # Need to advance clock as yielding does not involve sleep calls

            # Next iteration should sleep till market opens
            sleep.reset_mock()
            self.assertEqual(next(clock), (pd.Timestamp("2017-04-20 13:31", tz='UTC'), BAR))
            self.assertEqual(sleep.call_count, 45)  # sleep for 45 minutes till 13:30

            start_time = pd.Timestamp("2017-04-20 13:32:00", tz='UTC')
            # there are 6 hours and 29 minutes between 13:32 - 20:00
            for i in range(0, 389):
                self.assertEqual(next(clock), (start_time + pd.Timedelta('{}M'.format(i)), BAR))

            self.assertEqual(next(clock), (pd.Timestamp("2017-04-20 20:00:00", tz='UTC'), SESSION_END))

    def test_emit_frequency(self):
        # Test that events are yielded minutely
        with patch('zipline.gens.livetrading.pd.to_datetime') as to_dt, \
             patch('zipline.gens.livetrading.sleep') as sleep:
            clock = iter(RealtimeClock(
                pd.Timedelta("0s"),
                self.sessions,
                self.opens,
                self.closes,
                days_at_time(self.sessions, time(8, 45), "US/Eastern"),
                True
            ))

            global current_clock
            current_clock = pd.Timestamp("2017-04-20 00:00", tz='UTC')

            def advance_clock(*args, **kwargs):
                global current_clock
                current_clock += pd.Timedelta('50sec')

            def get_clock(*args, **kwargs):
                global current_clock
                return current_clock

            to_dt.side_effect = get_clock
            sleep.side_effect = advance_clock


            events = defaultdict(list)
            for ts, event_type in clock:
                if len(events[event_type]):
                    time_diff = ts - events[event_type][-1]
                    self.assertEquals(time_diff, pd.Timedelta('1min'))

                events[event_type].append(ts)

    def test_time_skew(self):
        raise NotImplementedError()
