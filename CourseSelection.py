#region imports
from AlgorithmImports import *
#endregion
# CoarseSelection

from typing import List, Any
from datetime import timedelta, datetime, date
from time import perf_counter
from QuantConnect import *
from QuantConnect.Data.Market import TradeBar
from QuantConnect.Data.UniverseSelection import Universe
from QuantConnect.Algorithm import *
from QuantConnect.Indicators import *
from collections import deque
import statistics

from Utils import printSymbolList
from WindowAnalytics import *


class CoarseSelection:
    exclude: List[Any]
    coarse_symbols: List[Any]

    def __init__(self, algorithm, max_coarse_count, price_threshold, max_price_limit, dollar_volume_threshold):
        self.algorithm = algorithm
        self.max_coarse_count = max_coarse_count
        self.price_threshold = price_threshold
        self.max_price_limit = max_price_limit
        self.daily_dollar_volume_threshold = dollar_volume_threshold
        self.average_dollar_volume_threshold = dollar_volume_threshold
        self.history_lookback = timedelta(days=366)  # for 1 year analysis
        self.selection_time = self.algorithm.Time - timedelta(1)

        self.market_symbol = None

        # state variables
        self.phase1dataBySymbol = dict()
        self.dataBySymbol = dict()
        self.algorithm.coarseDataBySymbol = self.dataBySymbol  # make indicator data available globally
        self.betaDataBySymbol = dict()
        self.coarse_symbols = []

        # get special lists
        handpicked_spreadsheet = self.algorithm.Download(
            "https://docs.google.com/spreadsheets/d/1gmDknrJZCjeX6xUhecq58sdLK9ss5ystDvtT-ywfktA/gviz/tq?tqx=out:csv")
        exclude_spreadsheet = self.algorithm.Download(
            "https://docs.google.com/spreadsheets/d/1UJQwddVoe2MneP1Bbs-v406MtYO5iQVu_gV5ItE0RPY/gviz/tq?tqx=out:csv")

        self.handpicked = []
        for row in handpicked_spreadsheet.split('\n'):
            if len(row) > 0:
                self.handpicked.append(row.replace('\"', ''))
        self.algorithm.Log(f'*** handpicked list: {self.handpicked}')

        self.exclude = []
        for row in exclude_spreadsheet.split('\n'):
            if len(row) > 0:
                self.exclude.append(row.replace('\"', ''))
        self.algorithm.Log(f'*** exclude list: {self.exclude}\n')

    def CoarseSelectionFunction(self, coarse):
        """
        Implements a multiphase approach to producing a coarse selection list
        1) compute smoothed daily metrics on the entire set of coarse stocks to avoid daily spikes
           e.g. sma(dollar_volume)
           get history for the smoothing period
        2) filter the list based on certain selection parameters
        3) get 1 year back history and compute detailed metrics to use in final selection
        :param coarse: QC provided list of all stocks
        :return: list of coarse selected stocks
        """

        #Makes selection only run once a month
        if self.algorithm.Time < self.selection_time:
            return []
        self.selection_time = self.algorithm.Time + timedelta(30)
        # phase 1 processed daily
        phase1_period = 9  # days to smooth
        # create a list of new phase1 symbols for every day
        phase1_list = list()
        for cf in coarse:
            if (((cf.HasFundamentalData and
                    self.price_threshold <= cf.Price <= self.max_price_limit and
                    cf.DollarVolume >= self.daily_dollar_volume_threshold and
                    cf.Symbol.Value not in self.exclude) or
                    cf.Symbol.Value in self.handpicked) and
                    cf.Symbol not in self.phase1dataBySymbol):
                phase1_list.append(cf.Symbol)
            # special handling for market symbol
            if (cf.Symbol.Value == self.algorithm.market_ticker and
                    cf.Symbol not in self.phase1dataBySymbol):
                self.algorithm.market_symbol = cf.Symbol
                self.market_symbol = cf.Symbol
                phase1_list.append(cf.Symbol)

        self.algorithm.Log("AMOUNT OF COARSE STOCKS IN UNIVERSE: {}".format(len(phase1_list)))

        # backfill all new phase 1 data (phase1_list)
        histories = self.algorithm.History(phase1_list, phase1_period, Resolution.Daily)
        # update existing phase1 data and create data for new phase1 symbols
        for cf in coarse:
            # update data for all known phase1 symbols
            # TODO: may need to rotate symbols out to manage memory?
            # TODO: prune any that no longer meet the dollar_volume threshold
            if cf.Symbol in self.phase1dataBySymbol:
                data = self.phase1dataBySymbol[cf.Symbol]
                data.update(cf.EndTime, cf.AdjustedPrice, cf.Volume)

            # process new phase1 symbols
            if cf.Symbol in phase1_list:
                if cf.Symbol not in self.phase1dataBySymbol:
                    self.phase1dataBySymbol[cf.Symbol] = \
                        Phase1SelectionData(self.algorithm, cf, phase1_period)
                    data = self.phase1dataBySymbol[cf.Symbol]
                    data.backfill(histories.loc[cf.Symbol])

        # ----------------------------------------------------------------------
        # check if need to rebalance
        if not self.algorithm.rebalance_flag:
            return Universe.Unchanged

        # self.algorithm.Log(f'*start rebalance. phase1data size:{len(self.phase1dataBySymbol)}')
        # filter to reduce the size of the initial population
        filtered = []
        for symbol, data in self.phase1dataBySymbol.items():
            if (data.average_dollar_volume > self.average_dollar_volume_threshold or
                    symbol.Value in self.handpicked or
                    symbol == self.market_symbol):
                filtered.append(data.cf)

        # phase 3 -- compute detailed indicators for all filtered symbols
        filtered_symbols = [x.Symbol for x in filtered]
        existing_symbols = list(self.dataBySymbol.keys())
        phase1_symbols = list(self.phase1dataBySymbol.keys())
        # produce a deduped combined list
        need_history = list(dict.fromkeys(filtered_symbols + existing_symbols))
        history_start = self.algorithm.Time - self.history_lookback
        history_end = self.algorithm.Time
        histories = self.algorithm.History(need_history,
                                        history_start, history_end, Resolution.Daily)

        # reset phase1 data for next rebalance
        self.phase1dataBySymbol.clear()

        # get full or add to existing history
        for symbol in need_history:
            if symbol not in self.dataBySymbol:
                new_symbol_data = CoarseSymbolData(self.algorithm, symbol)
                self.dataBySymbol[symbol] = new_symbol_data
                new_symbol_data.WarmUpIndicators(histories.loc[symbol])

                new_beta_data = BetaSymbolData(self.algorithm, symbol)
                self.betaDataBySymbol[symbol] = new_beta_data
                new_beta_data.WarmUpData(histories.loc[symbol])
            else:
                try:
                    # update existing data with new history
                    existing_symbol_data = self.dataBySymbol[symbol]
                    existing_symbol_data.AddToData(histories.loc[symbol])
                except KeyError:
                    self.algorithm.Log(f'* CoarseSelection KeyError {symbol.Value} ({symbol}) not found in dataBySymbol')
                    continue
                try:
                    existing_beta_data = self.betaDataBySymbol[symbol]
                    existing_beta_data.AddToData(histories.loc[symbol])
                except KeyError:
                    self.algorithm.Log(f'* CoarseSelection KeyError {symbol.Value} ({symbol}) not found in betaDataBySymbol')
                    continue

        # symbol data summary
        data_symbols_list = list(self.dataBySymbol.keys())
        net_new_set = set(need_history) - set(existing_symbols)
        net_new_list = list(net_new_set)
        # printSymbolList(self.algorithm, "* Coarse all phase1 symbols", phase1_symbols)
        # printSymbolList(self.algorithm, "* Coarse all data symbols", data_symbols_list)
        printSymbolList(self.algorithm, "* Coarse new candidates", net_new_list)

        price_benchmark = self.dataBySymbol[self.market_symbol].price_slow_absolute_slope
        volume_benchmark = self.dataBySymbol[self.market_symbol].average_dollar_volume_slope
        baseline = self.dataBySymbol[self.market_symbol].baseline_slope
        phase3_time = perf_counter()

        for symbol, x in self.dataBySymbol.items():
            # if symbol data not ready then skip further processing
            if not x.isReady:
                continue

            if x.price >= x.fast_signal:
                x.price_above_fast_signal = True
            else:
                x.price_above_fast_signal = False

            if x.price_slow_absolute_slope >= 0:
                x.price_above_zero = True
            else:
                x.price_above_zero = False

            # price slope is greater than SPY
            if x.price_slow_absolute_slope >= price_benchmark:
                x.price_above_benchmark = True
            else:
                x.price_above_benchmark = False

            # volume slope is greater than SPY
            if x.average_dollar_volume_slope >= volume_benchmark:
                x.volume_above_benchmark = True
            else:
                x.volume_above_benchmark = False

            # EMA200 or similar
            if x.baseline_slope > 0:
                x.baseline_slope_uptrend = True
            else:
                x.baseline_slope_uptrend = False

            # price variance above the line makes it actionable
            if x.price_variance >= 0:
                x.price_variance_above_line = True
            else:
                x.price_variance_above_line = False

            # kumo is green, tenkan and kijun are above bottom of kumo
            if x.senkouA >= x.senkouB and (x.tenkan > x.senkouB or x.kijun > x.senkouB):
                x.tenkan_kijun_above_kumo = True

            # kumo is red, tenkan and kijun are above bottom of kumo
            elif x.senkouB > x.senkouA and (x.tenkan > x.senkouA or x.kijun > x.senkouA):
                x.tenkan_kijun_above_kumo = True

            # tenkan and kijun are not above kumo
            else:
                x.tenkan_kijun_above_kumo = False

            if x.senkouA > x.senkouB:
                x.kumo_is_green = True
            else:
                x.kumo_is_green = False

            # tenkan and kijun are inside kumo
            if (x.senkouA > x.senkouB and x.tenkan <= x.senkouA and x.kijun <= x.senkouA and x.tenkan >= x.senkouB and x.kijun >= x.senkouB) or \
                    (x.senkouA < x.senkouB and x.tenkan >= x.senkouA and x.kijun >= x.senkouA and x.tenkan <= x.senkouB and x.kijun <= x.senkouB):
                x.tenkan_kijun_inside_kumo = True
            else:
                x.tenkan_kijun_inside_kumo = False


        # Calculate beta and stock_sortino_ratio - save in CoarseSymbolData so it can be used as a filter
        # Setup market_return for beta calculation
        market_prices = self.betaDataBySymbol[self.market_symbol].prices
        for symbol, data in self.betaDataBySymbol.items():
            beta_value = data.beta(market_prices)
            self.dataBySymbol[symbol].beta = beta_value
            sortino_value = data.stock_sortino_ratio()
            self.dataBySymbol[symbol].stock_sortino_ratio = sortino_value

        # diagnostics
        # for x in self.dataBySymbol.values():
        #    if x.symbol.Value == 'AMC' or x.symbol.Value == 'MRNA':
        #        self.algorithm.Log('** SINGLE SYMBOL DIAGNOSTIC **')
        #        message = '{}  is_uptrend: {}  Beta: {:.2f}  average_trend: {:.2f}  average_dollar_volume: ${:.2f}B  avg_dollar_volume_slope: {:.2f}  price_slow_slope: {:.2f}  price_diff_slope: {:.2f}  fast_absolute_slope: {:.2f}  slow_absolute_slope: {:.2f}  pvt_slow_slope: {:.2f}  pvt_slow_absolute_slope: {:.2f}  pvt_diff_slope: {:.2f}' \
        #            .format(x.symbol.Value, x.is_uptrend, x.beta, x.average_trend, x.average_dollar_volume / 1e9, x.avg_dollar_volume_slope, x.price_slow_slope, x.price_diff_absolute_slope, x.fast_absolute_slope, x.slow_absolute_slope, x.pvt_slow_slope, x.pvt_slow_absolute_slope, x.pvt_diff_slope)
        #        self.algorithm.Log(message)

        # apply 2nd order filter using CoarseSymbolData indicators
        selected = list(filter(lambda x:
                                x.isReady and
                                # not x.price_variance_above_line,
                                # x.is_uptrend and
                                x.volume_above_benchmark and 
                                #(x.price_above_benchmark or x.volume_above_benchmark) and
                                #x.price_above_zero and
                                # x.fast_signal > x.baseline_signal and
                                # x.price_diff_absolute_slope >= 0,
                                # x.pvt_slow_absolute_slope != float('nan') and
                                # x.price_variance >= 0 and
                                # x.price_variance_absolute_slope >= 0 and
                                # x.price_meta_variance_absolute_slope >= 0 and
                                # ((x.tenkan >= x.kijun) or x.tenkan_kijun_above_kumo) and
                                x.tenkan_kijun_inside_kumo and
                                # x.tenkan_above_kijun_signal >= 0 and
                                # x.beta >= 1 and
                                # x.price >= (x.fifty_two_week_high - (x.fifty_two_week_high * 0.03)),
                                (x.stock_sortino_ratio == float('inf') or x.stock_sortino_ratio == float('nan') or x.stock_sortino_ratio > 0),  # added logic to deal with some non-math results that come out of PortfolioMetrics
                                list(self.dataBySymbol.values())))
        self.algorithm.filter_criteria.append("Ichimoku")
        selected.sort(key=lambda x: x.average_dollar_volume, reverse=True)


        # selected symbols logging
        if self.algorithm.log_coarse_selected:
            self.algorithm.Log('** SELECTED SYMBOLS **')
            message = 'Benchmark Price Slope {:.2f}  Benchmark Volume Slope {:.2f}  Baseline Slope {:.2f}' \
                .format(price_benchmark, volume_benchmark, baseline)
            self.algorithm.Log(message)
            for x in selected[:self.max_coarse_count]:
                message = '{}  is_uptrend: {}  52WeekHigh: {:.2f}   Beta: {:.2f}   Sortino: {:.2f}   trend: {:.2f}  trend slope: {:.2f}   ' \
                        'avg $ vol: {:.2f} ' \
                        'price slope: {:.2f}   volume slope: {:.2f}   price diff: {:.2%}   abs price diff slope: {:.2f}   abs price variance slope: {:.2f}  ' \
                        'pvt slope: {:.2f}   abs pvt diff slope: {:.2f}   ' \
                        'price_above_benchmark {}  volume_above_benchmark {}  basline_slope_uptrend {}  tenkan above kijun {}  tenkan & kijun above kumo {}' \
                    .format(x.symbol.Value, x.is_uptrend, x.fifty_two_week_high, x.beta, x.stock_sortino_ratio, x.average_trend,
                            x.average_dollar_volume/1e9,
                            x.trend_absolute_slope, 
                            x.slow_absolute_slope, x.average_dollar_volume_slope, 
                            x.price_diff_pct, x.price_diff_absolute_slope, x.price_variance_absolute_slope, 
                            x.pvt_slow_absolute_slope, x.pvt_diff_absolute_slope,
                            x.price_above_benchmark, x.volume_above_benchmark, x.baseline_slope_uptrend,
                            x.tenkan >= x.kijun, x.tenkan_kijun_above_kumo)
                self.algorithm.Log(message)

        # candidate symbols logging
        if self.algorithm.log_coarse_candidates:
            self.algorithm.Log('** CANDIDATE SYMBOLS **')
            message = 'Benchmark Price Slope {:.2f}  Benchmark Volume Slope {:.2f}  Baseline Slope {:.2f}' \
                .format(price_benchmark, volume_benchmark, baseline)
            self.algorithm.Log(message)
            sorted_symbols = dict(
                sorted(self.dataBySymbol.items(),
                    key=lambda kv: kv[1].trend_absolute_slope,
                    reverse=True))

            all_symbols = [x for x in sorted_symbols.keys()]
            printSymbolList(self.algorithm, '* Coarse candidates', all_symbols)

            for symbol, x in sorted_symbols.items():
                if not x.isReady:
                    continue
                message = '{}  is_uptrend: {}  Beta: {:.2f}   Sortino: {:.2f}   trend: {:.2f}  trend slope: {:.2f}   ' \
                        'avg $ vol: {:.2f} ' \
                        'price slope: {:.2f}   volume slope: {:.2f}   pvt slope: {:.2f}   price diff: {:.2%}   abs price diff slope: {:.2f}   abs pvt diff slope: {:.2f}   ' \
                        'price_above_benchmark {}  volume_above_benchmark {}  basline_slope_uptrend {}  tenkan above kijun {}  tenkan & kijun above kumo {}' \
                    .format(x.symbol.Value, x.is_uptrend, x.beta, x.stock_sortino_ratio, x.average_trend,
                            x.average_dollar_volume/1e9, 
                            x.trend_absolute_slope,
                            x.slow_absolute_slope, x.average_dollar_volume_slope, x.pvt_slow_absolute_slope,
                            x.price_diff_pct, x.price_diff_absolute_slope, x.pvt_diff_absolute_slope,
                            x.price_above_benchmark, x.volume_above_benchmark, x.baseline_slope_uptrend,
                            x.tenkan >= x.kijun, x.tenkan_kijun_above_kumo)
                self.algorithm.Log(message)

        # csv diagnostic
        # sorted_symbols = dict(sorted(self.dataBySymbol.items(), key=lambda kv: (kv[1].trend_absolute_slope), reverse=True))
        # for symbol, x in sorted_symbols.items():
        #     message = ',_coarse,{},{},{:.2f},{:.2f},{:.2f},{:.2f}'\
        #         ',{:.2f},{:.2f},{:.2f},{:.2%},{:.2f},{:.2f}'\
        #         ',{},{},{},{},{}' \
        #         .format(x.symbol.Value, x.is_uptrend, x.beta, x.stock_sortino_ratio, x.average_trend, x.trend_absolute_slope, 
        #                 x.slow_absolute_slope, x.average_dollar_volume_slope, x.pvt_slow_absolute_slope, x.price_diff_pct, x.price_diff_absolute_slope, x.pvt_diff_absolute_slope,
        #                 x.price_above_benchmark, x.volume_above_benchmark, x.baseline_slope_uptrend, x.tenkan >= x.kijun, x.tenkan_kijun_above_kumo)
        #     self.algorithm.Log(message)

        # prepare the return list for the selected symbols
        self.coarse_symbols = [x.symbol for x in selected[:self.max_coarse_count]]

        # summary logging
        printSymbolList(self.algorithm, '* Coarse selected', self.coarse_symbols)
        return self.coarse_symbols


class CoarseSymbolData:
    """
    Update indicators daily by processing daily history via WarmUpIndicators() or AddToData()
    WarmUpIndicators() is used for new symbols
    AddToData is used for existing symbols filled forward based on latest data time marker
    """

    def __init__(self, algorithm, symbol):
        self.algorithm = algorithm
        self.symbol = symbol
        self.last_data_time = None  # used to update data history incrementally
        self.price = 0
        self.isReady = False        # are indicators ready to be used
        
        self.market_symbol_tenkan_above_kijun = False

        # state variable
        self.average_dollar_volume = 0
        self.is_uptrend = False
        self.trend = -1
        self.trend_slope = 0
        self.average_trend = 0  # average over trend_period
        self.pvt = 0  # price volume trend

        self.fast_signal = 0
        self.slow_signal = 0
        self.baseline_signal = 0

        self.price_above_fast_signal = False

        self.price_differential = 0

        self.price_fast_slope = 0
        self.price_slow_slope = 0
        self.price_diff_slope = 0
        self.pvt_fast_slope = 0
        self.pvt_slow_slope = 0
        self.price_diff_pct = 0
        self.pvt_diff_slope = 0

        self.price_fast_absolute_slope = 0
        self.price_slow_absolute_slope = 0
        self.avg_dollar_volume_slope = 0
        self.average_dollar_volume_slope = 0

        self.price_variance = 0.0
        self.price_variance_slope = 0.0
        self.price_variance_above_line = False

        self.tenkan = 0.0
        self.kijun = 0.0
        self.senkouA = 0.0
        self.senkouB = 0.0

        self.atr_value = 0.0
        self.atr_pct = 0.0

        self.tenkan_kijun_above_kumo = False
        self.tenkan_kijun_inside_kumo = False
        self.tenkan_above_kijun_signal = 0
        self.kumo_is_green = False

        self.price_above_benchmark = False
        self.volume_above_benchmark = False

        self.baseline_slope_uptrend = False

        self.price_area = float('nan')

        high_period = 253 #52 weeks
        self.price_value_window = RollingWindow[float](high_period)
        self.fifty_two_week_high = 0.0
        self.fifty_two_week_low = 0.0

        self.previous_close = 1

        # implemented in BetaSymbolData class
        self.beta = float('nan')
        self.stock_sortino_ratio = float('nan')

        # note: Coarse data is updated daily

        fast_period = 20
        slow_period = 50
        self.fast = ExponentialMovingAverage(fast_period)
        self.slow = ExponentialMovingAverage(slow_period)

        trend_period = 20
        trend_slope_period = 16
        self.avg_trend = ExponentialMovingAverage(trend_period)
        # Setup the window and result variable to use the window_slope function
        self.trend_absolute_slope = float('nan')
        self.trend_absolute_slope_window = RollingWindow[float](trend_slope_period)

        volume_period = 20
        volume_slope_period = 9
        self.avg_dollar_volume = ExponentialMovingAverage(volume_period)
        # Setup the window and result variable to use the window_slope function
        self.avg_dollar_volume_slope = float('nan')
        self.avg_dollar_volume_window = RollingWindow[float](volume_slope_period)

        baseline_period = 200
        baseline_slope_period = 20
        self.baseline = ExponentialMovingAverage(baseline_period)

        self.baseline_value_window = RollingWindow[float](baseline_slope_period)
        # Setup the window and result variable to use the window_slope function
        self.baseline_slope = float('nan')
        self.baseline_window = RollingWindow[float](baseline_slope_period)

        fast_slope_period = 20
        self.fast_value_window = RollingWindow[float](fast_slope_period)
        self.fast_signal_scale = Scale(algorithm, delta=True)
        self.fast_signal_slope = Slope(algorithm, self.fast_signal_scale)
        # Setup the window and result variable to use the window_slope function
        self.fast_absolute_slope = float('nan')
        self.fast_absolute_signal_window = RollingWindow[float](fast_slope_period)

        slow_slope_period = 20
        self.slow_value_window = RollingWindow[float](slow_slope_period)
        self.slow_signal_scale = Scale(algorithm, delta=True)
        self.slow_signal_slope = Slope(algorithm, self.slow_signal_scale)
        # Setup the window and result variable to use the window_slope function
        self.slow_absolute_slope = float('nan')
        self.slow_absolute_signal_window = RollingWindow[float](slow_slope_period)

        # *** Price Diff and Variance analysis windows
        self.price_diff_scale = Scale(algorithm, delta=False)  # fast - slow
        self.price_diff = LineDiff(self, self.price_diff_scale)

        diff_slope_period = 20
        price_diff_slope_period = 9  # this number needs to be at least 10 for 2021 to get GME -- this is worrying
        self.price_diff_signal_value_window = RollingWindow[float](diff_slope_period)
        self.price_diff_signal_scale = Scale(algorithm, delta=True)
        self.price_diff_signal_slope = Slope(algorithm, self.price_diff_signal_scale)
        # Setup the window and result variable to use the window_slope function
        self.price_diff_absolute_slope = float('nan')
        self.price_diff_absolute_window = RollingWindow[float](price_diff_slope_period)

        diff_pct_period = 20
        price_variance_slope_period = 9
        self.price_diff_pct_value_window = RollingWindow[float](diff_pct_period)
        self.price_diff_pct_spot = 0.0
        # Setup the window and result variable to use the window_slope function
        self.price_variance_absolute_slope = float('nan')
        self.price_variance_absolute_window = RollingWindow[float](price_variance_slope_period)
        # Setup the META window and result variable to use the window_slope function
        self.price_meta_variance_absolute_slope = float('nan')
        self.price_meta_variance_absolute_window = RollingWindow[float](price_variance_slope_period)

        # *** PVT analysis windows ***

        pvt_fast_period = 20
        pvt_fast_slope_period = 9
        self.pvt_fast_value = 0
        self.pvt_fast_signal = 0
        self.pvt_fast = ExponentialMovingAverage(pvt_fast_period)
        self.pvt_fast_value_window = RollingWindow[float](pvt_fast_slope_period)
        self.pvt_fast_signal_scale = Scale(algorithm, delta=True)
        self.pvt_fast_signal_slope = Slope(algorithm, self.pvt_fast_signal_scale)

        pvt_slow_period = 50
        pvt_slow_slope_period = 9
        self.pvt_slow_value = 0
        self.pvt_slow_signal = 0
        self.pvt_slow = ExponentialMovingAverage(pvt_slow_period)
        self.pvt_slow_value_window = RollingWindow[float](pvt_slow_slope_period)
        self.pvt_slow_signal_scale = Scale(algorithm, delta=True)
        self.pvt_slow_signal_slope = Slope(algorithm, self.pvt_slow_signal_scale)
        # Setup the window and result variable to use the window_slope function
        self.pvt_slow_absolute_slope = float('nan')
        self.pvt_slow_absolute_signal_window = RollingWindow[float](pvt_slow_slope_period)

        self.pvt_diff_scale = Scale(algorithm, delta=False)  # pvt_trend - pvt_signal
        self.pvt_diff = LineDiff(self, self.pvt_diff_scale)

        pvt_diff_slope_period = 5
        self.pvt_diff_signal_value_window = RollingWindow[float](pvt_diff_slope_period)
        self.pvt_diff_signal_scale = Scale(algorithm, delta=True)
        self.pvt_diff_signal_slope = Slope(algorithm, self.pvt_diff_signal_scale)

        # Setup the window and result variable to use the window_slope function
        self.pvt_diff_absolute_slope = float('nan')
        self.pvt_diff_absolute_window = RollingWindow[float](pvt_diff_slope_period)

        pvt_diff_pct_period = 9
        self.pvt_diff_pct_spot = 0.0
        self.pvt_diff_pct_value_window = RollingWindow[float](pvt_diff_pct_period)
        self.pvt_diff_pct = 0.0

        self.pvt_diff_scale = Scale(algorithm, delta=False)  # fast - slow
        self.pvt_diff = LineDiff(self, self.pvt_diff_scale)

        # *** setup Ichimoku Indicator and data states ***

        TenkanPeriod = 9
        KijunPeriod = 26
        SenkouAPeriod = 26
        SenkouBPeriod = 52
        SenkouADelay = 26
        SenkouBDelay = 26
        self.ichimoku = IchimokuKinkoHyo(self.symbol, TenkanPeriod, KijunPeriod,
                                         SenkouAPeriod, SenkouBPeriod, SenkouADelay, SenkouBDelay)

        # keeping values of tenkan above kijun so we know that it is not just a transient blip
        tenkan_above_kijun_period = 3
        self.tenkan_above_kijun_window = RollingWindow[float](tenkan_above_kijun_period)


    def update(self, time, close, open, high, low, volume, dollar_volume):
        self.last_data_time = time
        self.price = close

        self.price_value_window.Add(close)
        self.fifty_two_week_high = max(list(self.price_value_window))
        self.fifty_two_week_low = min(list(self.price_value_window))
        
        # update indicators and compute results when all indicators ready
        a = self.fast.Update(time, close)
        b = self.slow.Update(time, close)
        c = self.avg_dollar_volume.Update(time, dollar_volume)

        d = self.baseline.Update(time, close)

        self.pvt = self.pvt + (dollar_volume * ((close - self.previous_close) / self.previous_close))
        self.previous_close = close

        e = self.pvt_fast.Update(time, self.pvt)
        f = self.pvt_slow.Update(time, self.pvt)

        tradeBar = TradeBar(time, self.symbol, open, high, low, close, volume, timedelta(days=1))
        g = self.ichimoku.Update(tradeBar)

        # do analysis once all indicators ready
        if a and b and c and d and e and f and g:
            self.isReady = True
            fast = self.fast.Current.Value
            slow = self.slow.Current.Value
            baseline = self.baseline.Current.Value


            self.price_differential = fast - slow

            self.trend = (fast - slow) / ((fast + slow) / 2.0)
            self.avg_trend.Update(time, self.trend)
            self.average_trend = self.avg_trend.Current.Value
            # update the fast_signal_window for use by the window_slope function
            self.trend_absolute_slope_window.Add(self.average_trend)
            self.trend_absolute_slope = window_slope(self, self.trend_absolute_slope_window)

            self.fast_value_window.Add(fast)
            self.fast_signal = WMA_signal(self.fast_value_window)
            self.fast_signal_scale.update(self.fast_signal)
            self.fast_signal_slope.update(self.fast_signal)
            # update the fast_signal_window for use by the window_slope function
            self.fast_absolute_signal_window.Add(self.fast_signal)
            self.fast_absolute_slope = window_slope(self, self.fast_absolute_signal_window)

            self.slow_value_window.Add(slow)
            self.slow_signal = WMA_signal(self.slow_value_window)
            self.slow_signal_scale.update(self.slow_signal)
            self.slow_signal_slope.update(self.slow_signal)
            # update the slow_signal_window for use by the window_slope function
            self.slow_absolute_signal_window.Add(self.slow_signal)
            self.slow_absolute_slope = window_slope(self, self.slow_absolute_signal_window)

            self.baseline_value_window.Add(baseline)
            self.baseline_signal = SMA_signal(self.baseline_value_window)
            # update the baseline_window for use by the window_slope function
            self.baseline_window.Add(baseline)
            self.baseline_slope = window_slope(self, self.baseline_window)

            self.price_diff_pct_spot = (fast - slow) / slow
            self.price_diff_pct_value_window.Add(self.price_diff_pct_spot)
            self.price_diff_pct = SMA_signal(self.price_diff_pct_value_window)
            self.price_variance = self.price_diff_pct_spot - self.price_diff_pct
            # update the Variance for use by the window_slope function
            self.price_variance_absolute_window.Add(self.price_variance)
            self.price_variance_absolute_slope = window_slope(self, self.price_variance_absolute_window)
            # update the META Variance for use by the window_slope function
            self.price_meta_variance_absolute_window.Add(self.price_variance_absolute_slope)
            self.price_meta_variance_absolute_slope = window_slope(self, self.price_meta_variance_absolute_window)

            self.price_diff_scale.update(fast - slow)
            self.price_diff.update(fast, slow)

            self.price_diff_signal_value_window.Add(fast - slow)
            self.price_diff_signal = WMA_signal(self.price_diff_signal_value_window)
            self.price_diff_signal_scale.update(self.price_diff_signal)
            self.price_diff_signal_slope.update(self.price_diff_signal)
            # update the slow_signal_window for use by the window_slope function
            self.price_diff_absolute_window.Add(fast - slow)
            self.price_diff_absolute_slope = window_slope(self, self.price_diff_absolute_window)

            # *** area analysis ***
            self.price_area = relative_area(9, self.fast_value_window,
                                            self.slow_value_window)

            self.pvt_fast_value = self.pvt_fast.Current.Value

            self.pvt_fast_value_window.Add(self.pvt_fast_value)
            self.pvt_fast_signal = SMA_signal(self.pvt_fast_value_window)
            self.pvt_fast_signal_scale.update(self.pvt_fast_signal)
            self.pvt_fast_signal_slope.update(self.pvt_fast_signal)

            self.pvt_slow_value = self.pvt_slow.Current.Value

            self.pvt_slow_value_window.Add(self.pvt_slow_value)
            self.pvt_slow_signal = SMA_signal(self.pvt_slow_value_window)
            self.pvt_slow_signal_scale.update(self.pvt_slow_signal)
            self.pvt_slow_signal_slope.update(self.pvt_slow_signal)
            # update the slow_signal_window for use by the window_slope function
            self.pvt_slow_absolute_signal_window.Add(self.pvt_slow_signal)
            self.pvt_slow_absolute_slope = window_slope(self, self.pvt_slow_absolute_signal_window)

            self.pvt_diff_scale.update(self.pvt_fast_value - self.pvt_slow_value)
            self.pvt_diff.update(self.pvt_fast_value, self.pvt_slow_value)

            self.pvt_diff_signal_value_window.Add(self.pvt_fast_value - self.pvt_slow_value)
            self.pvt_diff_signal = WMA_signal(self.pvt_diff_signal_value_window)
            self.pvt_diff_signal_scale.update(self.pvt_diff_signal)
            self.pvt_diff_signal_slope.update(self.pvt_diff_signal)

            # update the slow_signal_window for use by the window_slope function
            self.pvt_diff_absolute_window.Add(self.pvt_fast_value - self.pvt_slow_value)
            self.pvt_diff_absolute_slope = window_slope(self, self.pvt_diff_absolute_window)

            self.pvt_diff_pct_spot = (self.pvt_fast_value - self.pvt_slow_value) / self.pvt_slow_value
            self.pvt_diff_pct_value_window.Add(self.pvt_diff_pct_spot)
            self.pvt_diff_pct = SMA_signal(self.pvt_diff_pct_value_window)

            # update the avg_dollar_volume_window for use by the window_slope function
            self.avg_dollar_volume_window.Add(self.avg_dollar_volume.Current.Value)
            self.avg_dollar_volume_slope = window_slope(self, self.avg_dollar_volume_window)

            # update all windows with current values
            self.tenkan = self.ichimoku.Tenkan.Current.Value
            self.kijun = self.ichimoku.Kijun.Current.Value
            self.senkouA = self.ichimoku.SenkouA.Current.Value
            self.senkouB = self.ichimoku.SenkouB.Current.Value

            self.tenkan_above_kijun_window.Add(self.tenkan - self.kijun)
            self.tenkan_above_kijun_signal = SMA_signal(self.tenkan_above_kijun_window)



            #
            # set up the variables that will be used to filter course symbols
            #
            self.price_fast_slope = self.fast_signal_slope.magnitude
            self.price_slow_slope = self.slow_signal_slope.magnitude
            self.price_fast_absolute_slope = self.fast_absolute_slope
            self.price_slow_absolute_slope = self.slow_absolute_slope

            # self.price_diff_pct = self.price_diff_pct
            self.price_diff_slope = self.price_diff_signal_slope.magnitude
            # self.price_diff_absolute_slope = self.price_diff_absolute_slope

            self.pvt_fast_slope = self.pvt_fast_signal_slope.magnitude
            self.pvt_slow_slope = self.pvt_slow_signal_slope.magnitude
            # self.pvt_slow_absolute_slope = self.pvt_slow_absolute_slope
            self.pvt_diff_slope = self.pvt_diff_signal_slope.magnitude

            self.average_dollar_volume = self.avg_dollar_volume.Current.Value
            self.average_dollar_volume_slope = self.avg_dollar_volume_slope

            self.is_uptrend = self.average_trend > 0

            # diagnostics
            # self.algorithm.Log('** COARSE DAILY DIAGNOSTIC **')
            # message = ' {}   {:%y/%m/%d}   Variance Slope: {:.3f}   Meta Slope: {:.3f}  ' \
            #            .format(self.symbol.Value, time, self.price_variance_absolute_slope, self.price_meta_variance_absolute_slope)
            # self.algorithm.Log(message)

            return

    def WarmUpIndicators(self, history):
        for bar in history.itertuples():
            self.update(bar.Index, bar.close, bar.open, bar.high, bar.low, bar.volume, bar.close * bar.volume)

    # AddToData(histories.loc[symbol]
    def AddToData(self, history):
        for bar in history.itertuples():
            if bar.Index > self.last_data_time:
                self.update(bar.Index, bar.close, bar.open, bar.high, bar.low, bar.volume, bar.close * bar.volume)




class BetaSymbolData:
    def __init__(self, algorithm, symbol):
        self.algorithm = algorithm
        self.symbol = symbol
        self.last_data_time = None
        # self.window = RollingWindow[Decimal](2)
        # self.returns = deque(maxlen=252)    # 1 year daily % return
        self.prices = RollingWindow[float](252)  # 1 year time and daily closing prices

    def update(self, time, price):
        self.last_data_time = time
        if price != 0:
            self.prices.Add(price)

    def WarmUpData(self, history):
        for bar in history.itertuples():
            self.update(bar.Index, bar.close)

    # AddToData(histories.loc[symbol]
    def AddToData(self, history):
        for bar in history.itertuples():
            if bar.Index > self.last_data_time:
                self.update(bar.Index, bar.close)

    def get_monthly_returns(self, daily_data):
        """ convert daily data to monthly returns.
        Form monthly returns for 1 year (252 days) of data
        If not enough data prorate the 1 year period
        :param daily_data - RollingWindow of prices
        :return - RollingWindow with monthly returns
        """

        days_per_month = 21
        months = math.floor(daily_data.Count / days_per_month)

        monthly_returns = deque(maxlen=months)
        for i in range(0, months):
            # get daily index into price data
            close = days_per_month * i
            open = days_per_month * (i + 1) - 1
            try:
                month_gain = (daily_data[close] - daily_data[open]) / daily_data[open]
            except ZeroDivisionError:
                month_gain = 0.0
            monthly_returns.append(month_gain)
            # diagnostic
            # if self.symbol.Value == 'AMAT' or self.symbol.Value == 'SPY':
            #     close_date = times[close]
            #     open_date = times[open]
            #     message = f'*monthly {self.symbol.Value} open: {open_date} ${daily_data[open]:.2f}' \
            #               f' close: {close_date} ${daily_data[close]:.2f} gain: {month_gain:.4f}'
            #     self.algorithm.Log(message)

        monthly_returns.reverse()  # make latest data last

        # diagnostic
        # if self.symbol.Value == 'AMAT' or self.symbol.Value == 'SPY':
        #     message = f'*monthly_returns: {monthly_returns}'
        #     self.algorithm.Log(message)

        return monthly_returns

    def beta(self, market_prices):
        asset_returns = np.array(self.get_monthly_returns(self.prices))
        market_returns = np.array(self.get_monthly_returns(market_prices))

        try:
            cov_result = np.cov(asset_returns, market_returns)
            covariance = cov_result[0, 1]
            market_variance = cov_result[1, 1]
            asset_beta = covariance / market_variance
        except ValueError as e:
            # self.algorithm.Log(f'Value Exception in beta({self.symbol.Value}) - {e}')
            asset_beta = float('nan')
        except ZeroDivisionError as e:
            self.algorithm.Log(f'ZeroDivision Exception in beta({self.symbol.Value}) - {e}')
            asset_beta = float('nan')

        # diagnostics
        # if self.symbol.Value == 'AMAT':
        #     message = f"*beta {self.symbol.Value} last_data: {self.last_data_time} beta: {asset_beta}\n" \
        #               f" asset_returns: {asset_returns}\n" \
        #               f" market_returns: {market_returns}"
        #     self.algorithm.Log(message)

        return asset_beta

    def stock_sortino_ratio(self, risk_free_rate=0.0005):
        """
        1yr stock sortino_ratio using monthly data extracted from daily data
        :param risk_free_rate: nominally, 3mo T-bill rate
        :return: sortino_ratio
        """

        # form monthly returns for 1 year (252 days) of data
        monthly_returns = np.array(self.get_monthly_returns(self.prices))

        # get negative returns
        average_return = np.average(monthly_returns)
        negative_gains = [x for x in monthly_returns if x < 0]
        downside_stddev = float('nan')

        # compute Sortino ratio
        try:
            downside_stddev = statistics.pstdev(negative_gains)
            sortino = (average_return - risk_free_rate) / downside_stddev
        except statistics.StatisticsError:
            sortino = float('nan')
        except ZeroDivisionError:
            sortino = float('nan')

        # diagnostic
        # message = (f'*sortino {self.symbol.Value}'
        #            f' avg_returns: {average_return:.2%} downside_stddev: {downside_stddev:.4%}'
        #            f' len(monthly_returns): {len(monthly_returns)}'
        #            f' len(-gains): {len(negative_gains)} sortino: {sortino:.2f}')
        # self.algorithm.Log(message)

        return sortino


class Phase1SelectionData(object):
    def __init__(self, algortihm, cf, period):
        self.algorithm = algortihm
        self.cf = cf
        self.symbol = cf.Symbol
        self.sma = SimpleMovingAverage(period)
        self.average_dollar_volume = 0.0

    def backfill(self, history):
        for bar in history.itertuples():
            self.update(bar.Index, bar.close, bar.volume)

    def update(self, time, price, volume):
        if self.sma.Update(time, price * volume):
            self.average_dollar_volume = self.sma.Current.Value
