"""
Module for building a complete dataset from local directory with csv files.
"""
import os
import glob

import logbook
import requests
from retry import retry
from numpy import empty, NaN
from pandas import DataFrame, read_csv, Index, Timedelta, NaT, concat
from pandas_datareader.data import DataReader
from pandas_datareader._utils import RemoteDataError

from zipline.utils.calendars import register_calendar_alias
from zipline.data.resample import minute_frame_to_session_frame
from zipline.utils.cli import maybe_show_progress

from . import core as bundles

logger = logbook.Logger(__name__)


def get_all_symbols(directory):
    symbols = set()
    for pattern in ['%s/HC*iqfeed.csv' % directory,
                    '%s/HC*iqfeed.csv.gz' % directory,
                    '%s/HC*ib.csv' % directory,
                    '%s/HC*ib.csv.gz' % directory]:
        files = glob.glob(pattern)
        for file_ in files:
            symbols.add(
                os.path.basename(file_)
                .replace('HC-', '', 1).split('-1M-')[0])
    return symbols


def load_csvs(csv_dir, symbol):
    all_dfs = [load_ib_csvs(csv_dir, symbol),
               load_iqfeed_csvs(csv_dir, symbol)]
    concatenated = concat(all_dfs).sort_index()
    deduplicated = concatenated \
        .reset_index() \
        .drop_duplicates(subset='date', keep='last') \
        .set_index('date')

    return deduplicated.sort_index()


def load_ib_csvs(csv_dir, symbol):
    all_dfs = []
    all_files = glob.glob('%s/HC-%s-*ib.csv' % (csv_dir, symbol)) + \
        glob.glob('%s/HC-%s-*ib.csv.gz' % (csv_dir, symbol))
    for file_ in all_files:
        df = read_csv(file_, parse_dates=[0], infer_datetime_format=True,
                      index_col=0)
        all_dfs.append(df)

    if not all_dfs:
        return

    merged_df = concat(all_dfs)
    merged_df.sort_index(inplace=True)

    return merged_df


def load_iqfeed_csvs(csv_dir, symbol):
    all_dfs = []
    all_files = glob.glob('%s/HC-%s-*iqfeed.csv' % (csv_dir, symbol)) + \
        glob.glob('%s/HC-%s-*iqfeed.csv.gz' % (csv_dir, symbol))
    for file_ in all_files:
        df = read_csv(file_, parse_dates=[0], infer_datetime_format=True,
                      index_col=0)
        all_dfs.append(df)

    if not all_dfs:
        return

    merged_df = concat(all_dfs)

    # Convert from US/Eastern to UTC Naive
    merged_df.index = \
        merged_df.index.tz_localize('US/Eastern').tz_convert(None)
    merged_df.sort_index(inplace=True)

    # Convert cumsum volumes to per bar ones:
    # IQFeed CSV file accumulates the volume over the day, restarting every
    # day. We need to calculate the difference between the previous and the
    # current bar. As the df contains multiple days grouping is needed to not
    # to carry values from one day to another. The fillna ensures that there
    # will be no nan values at the beginning of each day.
    merged_df['Date'] = merged_df.index.date
    merged_df['Volume'] = \
        merged_df.groupby('Date')['Volume'].diff().fillna(merged_df['Volume'])
    merged_df.drop(['Date'], inplace=True, axis=1)

    merged_df.index.name = 'date'  # Was DateTime before
    merged_df.rename(
        columns={
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume',
        },
        inplace=True,
    )

    return merged_df


@retry(tries=3, delay=1, backoff=2)
def RetryingDataReader(*args, **kwargs):
    return DataReader(*args, **kwargs)


def csvdir_equities(tframes=None, csvdir=None, daily_from_minute=True):
    """
    Generate an ingest function for custom data bundle
    This function can be used in ~/.zipline/extension.py
    to register bundle with custom parameters, e.g. with
    a custom trading calendar.
    Parameters
    ----------
    tframe: tuple, optional
        The data time frames, supported timeframes: 'daily' and 'minute'
    csvdir : string, optional, default: CSVDIR environment variable
        The path to the directory of this structure:
        <directory>/<timeframe1>/<symbol1>.csv
        <directory>/<timeframe1>/<symbol2>.csv
        <directory>/<timeframe1>/<symbol3>.csv
        <directory>/<timeframe2>/<symbol1>.csv
        <directory>/<timeframe2>/<symbol2>.csv
        <directory>/<timeframe2>/<symbol3>.csv
    Returns
    -------
    ingest : callable
        The bundle ingest function
    Examples
    --------
    This code should be added to ~/.zipline/extension.py
    .. code-block:: python
       from zipline.data.bundles import csvdir_equities, register
       register('custom-csvdir-bundle',
                csvdir_equities(["daily", "minute"],
                '/full/path/to/the/csvdir/directory'))
    """

    return CSVDIRBundle(tframes, csvdir, daily_from_minute).ingest


class CSVDIRBundle:
    """
    Wrapper class to call csvdir_bundle with provided
    list of time frames and a path to the csvdir directory
    """

    def __init__(self, tframes=None, csvdir=None, daily_from_minute=True):
        self.tframes = tframes
        self.csvdir = csvdir
        self.daily_from_minute = daily_from_minute

    def ingest(self,
               environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir):

        csvdir_bundle(environ,
                      asset_db_writer,
                      minute_bar_writer,
                      daily_bar_writer,
                      adjustment_writer,
                      calendar,
                      start_session,
                      end_session,
                      cache,
                      show_progress,
                      output_dir,
                      self.tframes,
                      self.csvdir,
                      self.daily_from_minute)


@bundles.register("csvdir")
def csvdir_bundle(environ,
                  asset_db_writer,
                  minute_bar_writer,
                  daily_bar_writer,
                  adjustment_writer,
                  calendar,
                  start_session,
                  end_session,
                  cache,
                  show_progress,
                  output_dir,
                  tframes=None,
                  csvdir=None,
                  daily_from_minute=True):
    """
    Build a zipline data bundle from the directory with csv files.
    """
    if not csvdir:
        csvdir = environ.get('ZIPLINE_CSV_DIR')
        if not csvdir:
            raise ValueError("ZIPLINE_CSV_DIR environment variable is unset")

    if not os.path.isdir(csvdir):
        raise ValueError("%s is not a directory" % csvdir)

    if not tframes:
        tframes = set(["daily", "minute"]).intersection(os.listdir(csvdir))

        if not tframes:
            raise ValueError("'daily' and 'minute' directories "
                             "not found in '%s'" % csvdir)

    divs_splits = {
        'dividends': DataFrame(columns=[
            'sid', 'amount', 'ex_date',
            'record_date', 'declared_date', 'pay_date']),
        'splits': DataFrame(columns=['sid', 'ratio', 'effective_date'])
    }

    for tframe in tframes:
        ddir = os.path.join(csvdir, tframe)

        symbols = sorted(get_all_symbols(ddir))
        if not symbols:
            raise ValueError("no ingestable CSV files found in %s" % ddir)

        dtype = [('start_date', 'datetime64[ns]'),
                 ('end_date', 'datetime64[ns]'),
                 ('auto_close_date', 'datetime64[ns]'),
                 ('symbol', 'object')]
        metadata = DataFrame(empty(len(symbols), dtype=dtype))

        if tframe == 'minute':
            writer = minute_bar_writer
        else:
            writer = daily_bar_writer

        writer.write(_pricing_iter(ddir, symbols, metadata,
                     divs_splits, show_progress, calendar,
                     daily_from_minute=False),
                     show_progress=show_progress)

        if tframe == 'minute' and daily_from_minute:
            logger.info("Deriving daily bars from minute data")
            daily_bar_writer.write(
                _pricing_iter(ddir, symbols, metadata,
                              divs_splits, show_progress, calendar,
                              daily_from_minute=True),
                show_progress=show_progress)

        # Hardcode the exchange to "CSVDIR" for all assets and (elsewhere)
        # register "CSVDIR" to resolve to the NYSE calendar, because these
        # are all equities and thus can use the NYSE calendar.
        metadata['exchange'] = "CSVDIR"

        asset_db_writer.write(equities=metadata)

        with requests.Session() as session:
            splits, dividends = \
                download_splits_and_dividends(symbols, metadata, session)

        adjustment_skip = environ.get('ZIPLINE_CSV_DIR_ADJUSTMENT_SKIP')
        if adjustment_skip:
            if 'splits' in adjustment_skip.lower():
                logger.info("Skipping adjustment store: splits")
                splits = None
            if 'dividends' in adjustment_skip.lower():
                logger.info("Skipping adjustment store: dividends")
                dividends = None
            if dividends is not None and splits is not None:
                raise ValueError("Invalid value for "
                                 "ZIPLINE_CSV_DIR_ADJUSTMENT_SKIP: %s" %
                                 adjustment_skip)

        adjustment_writer.write(splits=splits, dividends=dividends)


def download_splits_and_dividends(symbols, metadata, session):
    adjustments = []
    for sid, symbol in enumerate(symbols):
        try:
            logger.debug("Downloading splits/dividends for %s" % symbol)
            df = RetryingDataReader(symbol,
                                    'yahoo-actions',
                                    metadata.ix[sid].start_date,
                                    metadata.ix[sid].end_date,
                                    session=session).sort_index()
        except RemoteDataError:
            logger.warning("No data returned from Yahoo for %s" % symbol)
            df = DataFrame(columns=['value', 'action'])

        df['sid'] = sid
        adjustments.append(df)

    adj_df = concat(adjustments)
    adj_df.index.name = 'date'
    adj_df.reset_index(inplace=True)

    splits_df = adj_df[adj_df['action'] == 'SPLIT']
    splits_df = splits_df.rename(
        columns={'value': 'ratio', 'date': 'effective_date'},
    )
    splits_df.drop('action', axis=1, inplace=True)
    splits_df.reset_index(inplace=True, drop=True)

    dividends_df = adj_df[adj_df['action'] == 'DIVIDEND']
    dividends_df = dividends_df.rename(
        columns={'value': 'amount', 'date': 'ex_date'},
    )
    dividends_df.drop('action', axis=1, inplace=True)
    # we do not have this data in the yahoo dataset
    dividends_df['record_date'] = NaT
    dividends_df['declared_date'] = NaT
    dividends_df['pay_date'] = NaT
    dividends_df.reset_index(inplace=True, drop=True)

    return splits_df, dividends_df


def _pricing_iter(csvdir, symbols, metadata, divs_splits, show_progress,
                  calendar, daily_from_minute=False):
    with maybe_show_progress(symbols, show_progress,
                             label='Loading custom pricing data: ') as it:
        for sid, symbol in enumerate(it):
            logger.debug('%s: sid %s' % (symbol, sid))

            dfr = load_csvs(csvdir, symbol)

            start_date = dfr.index[0]
            end_date = dfr.index[-1]

            # The auto_close date is the day after the last trade.
            ac_date = end_date + Timedelta(days=1)
            metadata.iloc[sid] = start_date, end_date, ac_date, symbol

            if 'split' in dfr.columns:
                tmp = 1. / dfr[dfr['split'] != 1.0]['split']
                split = DataFrame(data=tmp.index.tolist(),
                                  columns=['effective_date'])
                split['ratio'] = tmp.tolist()
                split['sid'] = sid

                splits = divs_splits['splits']
                index = Index(range(splits.shape[0],
                                    splits.shape[0] + split.shape[0]))
                split.set_index(index, inplace=True)
                divs_splits['splits'] = splits.append(split)

            if 'dividend' in dfr.columns:
                # ex_date   amount  sid record_date declared_date pay_date
                tmp = dfr[dfr['dividend'] != 0.0]['dividend']
                div = DataFrame(data=tmp.index.tolist(), columns=['ex_date'])
                div['record_date'] = NaT
                div['declared_date'] = NaT
                div['pay_date'] = NaT
                div['amount'] = tmp.tolist()
                div['sid'] = sid

                divs = divs_splits['dividends']
                ind = Index(range(divs.shape[0], divs.shape[0] + div.shape[0]))
                div.set_index(ind, inplace=True)
                divs_splits['dividends'] = divs.append(div)

            if daily_from_minute:
                dfr_daily = minute_frame_to_session_frame(dfr, calendar)
                calendar_sessions = \
                    calendar.sessions_in_range(start_date, end_date)
                missing_dates = calendar_sessions.difference(dfr_daily.index)

                for missing_date in missing_dates:
                    logger.warning('Forward filling data for %s: %s' %
                                   (symbol, str(missing_date)))
                    dfr_daily.loc[missing_date] = NaN

                # Need to sort as missing_date fillings are put at the
                # end of the dataframe
                dfr_daily.sort_index(inplace=True)

                yield sid, dfr_daily.fillna(method='pad')
            else:
                yield sid, dfr


register_calendar_alias("CSVDIR", "NYSE")
