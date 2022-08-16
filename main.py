#region imports
from AlgorithmImports import *
#endregion
'''
Multi-stock trading model
scb: 3/19/2021

Supports a list of single symbols
Only uses 1st symbol in the 'Confidence' plot
'''

# import QC dependencies
from QuantConnect import *
from QuantConnect.Indicators import *
from QuantConnect.Algorithm import *
from QuantConnect.Algorithm.Framework import *
from QuantConnect.Algorithm.Framework.Alphas import *
from QuantConnect.Algorithm.Framework.Portfolio import NullPortfolioConstructionModel
from QuantConnect.Algorithm.Framework.Risk import *
from QuantConnect.Algorithm.Framework.Selection import *
from QuantConnect.Algorithm.Framework.Execution import *
from QuantConnect.Data.Consolidators import *
from QuantConnect.Data.UniverseSelection import *
from QuantConnect.Orders import *
from QuantConnect.Securities import *
from datetime import date, datetime, timedelta, timezone
from QuantConnect.Python import *
from QuantConnect.Storage import *

QCAlgorithmFramework = QCAlgorithm
QCAlgorithmFrameworkBridge = QCAlgorithm

# include algorithm dependent classes
from CoarseSelection import CoarseSelection
from FineSelection import FineSelection
#from OitaAlphaDaily import *
# from OitaAlpha30min import *
#from YokkaichiAlpha import *
#from KawasakiAlpha import *
#from StockPortfolioConstructionModel import *
#from PortfolioAllocationModel import *
#from ETFRiskManagementModel import *
#from PortfolioMetrics import *
#from UniverseHistogram import *
from Utils import *
from pytz import timezone


class Proust(QCAlgorithm):
    """
    Designed to refresh the selected universe n-weekly
    """

    def Initialize(self):
        self.SetTimeZone("America/New_York")
        self.SetStartDate(2020, 1, 1)
        self.SetEndDate(2021, 1, 1)
        self.SetCash(100000)  # Set Strategy Cash
        self.run_starttime = self.Time      # save for calculating history from the start


        # Setup Rebalance and Reporting period
        self.period_units = 'week'      # 'month' is no longer supported
        self.n_periods = 4              # n_periods before rebalance

        self.period_count = 0
        self.period_format = "%Y-%m-%d"
        self.current_period = self.Time.strftime(self.period_format)
        self.previous_period = None     # set in scheduled events
        self.rebalance_flag = True     # set True in scheduled event to start a rebalance pass
        self.period_start = self.current_period
        self.period_end = None

        ########################
        # logging control
        ########################
        self.logger = Logger(self)           # Initialize insight logger
        self.log_orders = False              # log order and final insight info
        self.log_insights = True             # log firing related insight details if changing direction
        self.log_detail_insights = False     # log all final insights for specified interval
        self.detail_start = '2017-06-01'
        self.detail_end = '2017-07-01'
        self.log_coarse_candidates = False    # log symbol values for 1st filter selected
        self.log_coarse_selected = False      # log symbol values for 2nd filter selected
        self.log_fine_selected = False        # log symbol values for fundamental filter selected
        self.log_allocation = False           # log allocation details
        self.log_performance_summary = True   # log period performance report

        # portfolio_metrics.metricsBySymbol[symbol]->MetricData->metricsByPeriod[month]->PeriodMetricData
        # self.portfolio_metrics = PortfolioMetrics(self)
        # self.histogram = UniverseHistogram(self)
        # self.allocation_model = PortfolioAllocationModel(self, self.portfolio_metrics)
        self.coarseDataBySymbol = None  # set in CoarseSelection() to allow handle to indicator data
        # self.kawasakiDataBySymbol = None # for sharing data outside of Kawasaki
        self.IndicatorDataBySymbol = dict()

        # Universe filter parameters
        # TODO: make this a kwargs dictionary and pass into Coarse/Fine Selection
        max_coarse_count = 30   # avoid runaway filter conditions
        dollar_volume_threshold = 5e8  # 1e9 is $1B daily volume
        price_threshold = 10
        max_price_limit = 10000
        self.max_universe_size = 10     # max universe selected including keepers
        self.keep_percent = 0.5        # keep percent of existing but not selected stocks
        # Setup global Universe parameters
        self.UniverseSettings.Resolution = Resolution.Minute
        self.UniverseSettings.ExtendedMarketHours = False
        self.UniverseSettings.Leverage = 2

        # run information log
        utc_clock = datetime.now(timezone('UTC'))
        utc_time = utc_clock.strftime("%Y-%m-%d %H:%M:%S")
        local_time = utc_clock.astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d %H:%M:%S")
        dollar_volume_threshold_length = len(str(dollar_volume_threshold))
        self.Log(f'*** Backtest started @{local_time} PST  ({utc_time} UTC) <***')
        self.Log(f'*** period_units={self.period_units} n_periods={self.n_periods}')
        self.Log(f'*** max_coarse_count={max_coarse_count} max_universe_size={self.max_universe_size}'
                 f' keep_percent={self.keep_percent}')
        self.Log(f'*** dollar_volume_threshold={dollar_volume_threshold} ({dollar_volume_threshold_length})'
                  f' price_threshold={price_threshold}'
                  f' max_price={max_price_limit}')

        # Setup a market symbol to drive scheduling functions, beta and benchmark
        self.market_ticker = "SPY"
        self.spy = self.AddEquity(self.market_ticker, Resolution.Minute, 'USA', True, 1, False).Symbol
        self.market_symbol = None   # will be set in CoarseSelection to get the correct symbol value
        self.SetBenchmark(self.market_ticker)
        

        self.selected_stocks = list(list())   #list of list of monthly selected stocks
        self.monthly_data = list(list())      #list of list of monthly selected stocks performance over time
        self.monthly_percent_data = list(list())    #2d array of monthly selected portfolio vs percent change in price from starting price
        self.filter_criteria = list()
        self.spy_percent = list()

        self.chart = Chart("Monthly Data")    #Chart of monthly price of selected stocks
        self.AddChart(self.chart)
        self.chart.AddSeries(Series(SeriesType.Line, name="SPY"))

        self.percent_chart = Chart("Monthly Percent Change")    #Chart of percent increase price from price bought at to current price
        self.AddChart(self.percent_chart)
        self.percent_chart.AddSeries(Series(SeriesType.Line, name="SPY"))

        self.spy_intial_price = self.History(self.spy, 1, Resolution.Daily)['close'][0]
        
        cs = CoarseSelection(self, max_coarse_count,
                             price_threshold=price_threshold,
                             max_price_limit=max_price_limit,
                             dollar_volume_threshold=dollar_volume_threshold)
        fs = FineSelection(self)

        # AddUniverse
        self.AddUniverse(cs.CoarseSelectionFunction, fs.FineSelectionFunction)

        # set volatility equity
        #self.volatility_symbol = self.AddEquity("VIXY", Resolution.Minute, 'USA', True, 1, False).Symbol
        
        #self.execution = 0  # for plotting in PCM

        #########################
        # Setup the Alpha models
        #########################
        #self.AddAlpha(YokkaichiAlpha(self, self.volatility_symbol))
        #self.AddAlpha(OitaAlphaDaily(self))
        #self.AddAlpha(KawasakiAlpha(self))

        ######################################
        # Setup the other Framework components
        ######################################
        # Set Portfolio Construction Model
        #self.SetPortfolioConstruction(StockPortfolioConstructionModel(self, self.allocation_model))

        #self.SetExecution(ImmediateExecutionModel())

        # Set Risk Management Model
        maxdrawdown = 0.15
        highwater   = 0.15
        memespike   = 0.4
        #self.SetRiskManagement(ETFRiskManagmentModel(self, maxdrawdown, highwater, memespike))

        # Circuit Breaker feedback mechanism
        # set circuit_breaker[symbol] = True in RiskManagementModel
        # PortfolioManager ignore trades while circuit_breaker[symbol] = true
        # Reset circuit_breaker on new day
        self.circuit_breaker = dict()  # [symbol] -> boolean
        
        # Hard Stop feedback mechanism
        # used for Trailing Stops and SuperTrend
        # set hard_stop[symbol] = True in RiskManagementModel
        # PortfolioManager sends a down insight if hard_stop[symbol] = true
        # Reset hard_stop inside PortfolionManager
        self.hard_stop = dict()  # [symbol] -> boolean

        # Setup scheduled events
        # Only place trades when market is open but allow indicator data on extended hours
        self.market_is_open = False
        # suppress generating insights near market closing so related trades don't happen the next day
        self.suppress_insights = False

        self.Schedule.On(self.DateRules.EveryDay(self.market_ticker), self.TimeRules.AfterMarketOpen(self.market_ticker, 0),
                         self.OnMarketOpen)
        self.Schedule.On(self.DateRules.EveryDay(self.market_ticker), self.TimeRules.BeforeMarketClose(self.market_ticker, 10),
                         self.OnMarketClose)
        self.Schedule.On(self.DateRules.EveryDay(self.market_ticker), self.TimeRules.AfterMarketOpen(self.market_ticker, 0),
                         self.OnInsightsOpen)
        self.Schedule.On(self.DateRules.EveryDay(self.market_ticker), self.TimeRules.BeforeMarketClose(self.market_ticker, 30),
                         self.OnInsightsClosed)
        # set flags for rebalancing and metrics update
        self.Schedule.On(self.DateRules.WeekStart(), self.TimeRules.At(0, 0), self.OnWeekStart)
        self.Schedule.On(self.DateRules.WeekEnd(), self.TimeRules.At(16, 0), self.OnWeekEnd)

    def OnMarketOpen(self):
        self.market_is_open = True
        # clear circuit_breaker flag to allow trades
        for s in self.Securities:
            security = s.Value
            self.circuit_breaker[security.Symbol] = False

    def OnMarketClose(self):
        self.market_is_open = False

    def OnInsightsOpen(self):
        self.suppress_insights = False

    def OnInsightsClosed(self):
        self.suppress_insights = True

    def OnWeekStart(self):
        self.period_count += 1
        if self.period_count >= self.n_periods:  # move to next period
            self.period_count = 0
            self.previous_period = self.current_period
            self.current_period = self.Time.strftime(self.period_format)
            self.period_start = self.current_period
            #self.Log(f'>>Period Start: {self.period_start} <<')                        ////////////////////////
            active_list = [s.Value.Symbol for s in self.ActiveSecurities]
            #printSymbolList(self, "ActiveSecurities ", active_list)                    ////////////////////////
            self.rebalance_flag = True  # will be cleared in FineSelection
        return

    def OnWeekEnd(self):
        if self.period_count == self.n_periods - 1:
            self.period_end = self.Time.strftime(self.period_format)
            #self.Log(f'>>Period End: {self.period_end} <<')                           /////////////////////////
            # if self.current_period is not None:
            #     self.portfolio_metrics.update_metrics()
            #     if self.log_performance_summary:
            #         self.portfolio_metrics.print_report(self.period_end)
            return

    def OnEndOfAlgorithm(self):

        #csv logging
        #logging price csv
        self.Log("_obv Price of stocks by month,")
        for (price_list, filt) in zip(self.monthly_data, self.filter_criteria):
            message = "_obv " + filt + ","
            for price in price_list:
                message += str(price) + ","
            self.Log(message)
        self.Log("_obv Percent change of stock compared to purchase price,")
        for (percent_list, filt) in zip(self.monthly_percent_data, self.filter_criteria):
            message = "_obv " + filt + ","
            for percent in percent_list:
                message += str(percent) + ","
            self.Log(message)

        self.Log("_obv Percent change compared to SPY")
        avg = list()
        for (percent_list, spy_percent) in zip(self.monthly_percent_data, self.spy_percent):
            message = "_obv " + filt + ","
            average = 0
            for percent in percent_list:
                if percent is not None and spy_percent is not None:
                    message += str(percent - spy_percent) + ","
                    average += percent - spy_percent
            average /= len(percent_list)
            avg.append(average)
            self.Log(message)
        
        message = "_obv Average Percent Change,"
        for a in avg:
            message += str(a) + ","
        self.Log(message)
        if len(avg) > 0:
            avgavg = sum(avg) / len(avg)
            self.Log("_obv Percent change in portfolio compared to spy," + str(avgavg))


        self.Log(f'>> Algorithm End: {self.Time} <<')
        # TODO: rework histogram to handle week periods
        # self.histogram.print_histogram(self.portfolio_metrics)

    def OnOrderEvent(self, fill):
        # process order history recording and logging
        if fill.Status != OrderStatus.Filled:
            return
        #self.portfolio_metrics.update_order_transactions(self.Time, fill)

        if self.log_orders:
            # output final insight
            self.logger.output(fill.Symbol)
            # output order information
            if fill.FillQuantity < 0:
                message = 'Order Filled @{}: SELL {} {:.0f}sh @${:0.2f} = {:.2f} -- Total Portfolio Value: ${:0.2f}' \
                    .format(self.Time, fill.Symbol.Value, -fill.FillQuantity, fill.FillPrice,
                            fill.FillQuantity * fill.FillPrice, self.Portfolio.TotalPortfolioValue)
            else:
                message = 'Order Filled @{}: BUY {} {:.0f}sh @${:0.2f} = {:.2f} -- Total Portfolio Value: ${:0.2f}' \
                    .format(self.Time, fill.Symbol.Value, fill.FillQuantity, fill.FillPrice,
                            fill.FillQuantity * fill.FillPrice, self.Portfolio.TotalPortfolioValue)
            self.Log(message)
            self.Log('----------------------------------------------')
