#region imports
from AlgorithmImports import *
#endregion
'''
FineSelection
Input is a list of Symbols from the CoarseSelection phase
Returns a new list of Symbols that will populate the active universe

The intent is to apply additional filters using the available fundamental properties.
See: https://www.quantconnect.com/docs/data-library/fundamentals#Fundamentals-Morningstar-US-Equity-Data
'''

# import QC dependencies
from QuantConnect import *
from QuantConnect.Indicators import *
from QuantConnect.Algorithm import *
from QuantConnect.Algorithm.Framework import *
from QuantConnect.Algorithm.Framework.Alphas import *
from QuantConnect.Algorithm.Framework.Portfolio import *
from QuantConnect.Algorithm.Framework.Risk import *
from QuantConnect.Algorithm.Framework.Selection import *
from QuantConnect.Algorithm.Framework.Execution import *
from QuantConnect.Data.Consolidators import *
from QuantConnect.Data.UniverseSelection import *
from QuantConnect.Orders import *
from datetime import date, datetime, timedelta, timezone
from QuantConnect.Python import *
from QuantConnect.Storage import *
import datetime

QCAlgorithmFramework = QCAlgorithm
QCAlgorithmFrameworkBridge = QCAlgorithm

from UniverseHistogram import *
from Utils import *

import math

class FineSelection:
    def __init__(self, algorithm):
        self.algorithm = algorithm
        self.selection_time = self.algorithm.Time - timedelta(1)

        self.include_invested_in_keep = True    #  include invested in keep list

        # csv diagnostic header
        #msg = "log,_fine,symbol,price_diff_abs_slope,price_diff_slope,price_fast_slope,price_slow_slope," \
        #      "pvt_diff_abs_slope,fast_abs_slope,slow_abs_slope,trend_abs_slope"
        #self.algorithm.Log(msg)

    def FineSelectionFunction(self, fine_symbols):
        # Update universe symbols as determined by Scheduled events
        # if not self.algorithm.rebalance_flag:
        #     return Universe.Unchanged
        
        #makes selection run only once a month
        if self.algorithm.Time < self.selection_time:
            return Universe.Unchanged
        self.selection_time = self.algorithm.Time + timedelta(30)

        # calculate date for one year ago so we can filter out companies that IPOd since last year -- they screw up the Beta calculation
        one_year_ago = self.algorithm.Time - timedelta(days = 365)
        six_months_ago = self.algorithm.Time - timedelta(days = 182)
        three_months_ago = self.algorithm.Time - timedelta(days = 91)
        
        
        
        # if (x.SecurityReference.IPODate < three_months_ago) and (x.OperationRatios.RevenueGrowth.ThreeMonths > (x.OperationRatios.RevenueGrowth.OneYear/4)) and x.ValuationRatios.FCFPerShare >= 0:
        # if (x.SecurityReference.IPODate < three_months_ago) and x.ValuationRatios.PEGRatio < 3 and x.ValuationRatios.FCFYield >= 0:
        fundamental_symbols = dict()
        #for x in fine_symbols:
        #    if (x.SecurityReference.IPODate < three_months_ago) and x.OperationRatios.RevenueGrowth.ThreeMonths > 0:
        #        fundamental_symbols[x.Symbol] = x
        
        # (x.AssetClassification.MorningstarSectorCode == MorningstarSectorCode.Technology) and \       
        for x in fine_symbols:
            if      (x.SecurityReference.IPODate < three_months_ago):
                    fundamental_symbols[x.Symbol] = x
            
        #fine_symbols = dict(filter(lambda kv: ((kv[1].MarketCap > 10e9) and (kv[1].SecurityReference.IPODate < six_months_ago)), fine_symbols.items()))

        # setup join data structures for sorting
        join = dict()
        for symbol, fundamental_data in fundamental_symbols.items():
            join[symbol] = joindata(
                # get current fundamental data
                fundamental_data,
                # get current coarse data
                self.algorithm.coarseDataBySymbol[symbol]
            )
            
                            

        sortedByfactor1 = sorted(join.items(), 
                                key=lambda kv: (
                                                    kv[1].b.price_diff_pct_spot
                                                ), 
                                reverse=True)
        sortedByfactor2 = sorted(join.items(), 
                                key=lambda kv: (
                                                    kv[1].a.ValuationRatios.FCFYield
                                                ), 
                                reverse=True)
        sortedByfactor3 = sorted(join.items(), 
                                key=lambda kv: ( 
                                                    kv[1].a.OperationRatios.RevenueGrowth.ThreeMonths
                                                ), 
                                reverse=True)

        sortedByfactor4 = sorted(join.items(), 
                                key=lambda kv: ( 
                                                    kv[1].b.price / kv[1].b.fifty_two_week_high
                                                ), 
                                reverse=True)
        
        
        stock_dict = {}
        
        # assign a score to each stock, you can also change the rule of scoring here.
        for i,ele in enumerate(sortedByfactor1):
            rank1 = i
            rank2 = sortedByfactor2.index(ele)
            rank3 = sortedByfactor3.index(ele)
            rank4 = sortedByfactor4.index(ele)
            score = sum([rank1*0,rank2*0,rank3*0,rank4*1.0])
            stock_dict[ele] = score
        
        # sort the stocks by their scores
        fine_symbols = sorted(stock_dict.items(), key=lambda d:d[1],reverse=True)
        
        
        fine_list = [x[0][0] for x in fine_symbols]
        printSymbolList(self.algorithm, "*** fine list", fine_list)
         
         
        #logging fundemental data for symbols identified by course selection
        if self.algorithm.log_fine_selected:
            self.algorithm.Log('** FUNDAMENTAL DATA FOR SELECTED SYMBOLS **')
            for x in fine_symbols:
                message = '{}  {}  Revenue Growth 3M: {:.2f}   Revenue Growth 1Y: {:.2f}   EVToEBITDA: {:.2f}   ' \
                          'Basic EPS 12 months: {:.2f}    PE Ratio: {:.2f}    ' \
                          'Net Income: {:.2f}M    Return on Invested Capital: {:.2f}    Debt Equity Ratio: {:.2f}    ' \
                          'Free Cash Flow: {:.2f}   Free Cash Flow Yield: {:.2f}   ' \
                          'EPS 3M: {:.2f}   PEG Ratio: {:.2f}   First Year EPS Growth: {:.2f}   ' \
                          'price diff slope: {:.2f}   baseline slope: {:.2f}  price diff spot: {:.2f}   price diff pct: {:.2%}    ' \
                          'price area: {:.3f}   ' \
                          'price variance: {:.3f}  price variance slope: {:.3f}   meta variance slope: {:.3f}   price_variance_above_line {}   ' \
                          'pvt diff slope: {:.2f}   pvt diff pct: {:.2%}  pvt diff spot: {:.2f}   ' \
                          'tenkan_kijun_above_kumo: {}  kumo_is_green: {}   ' \
                    .format(x[0][0].Value, x[1], x[0][1].a.OperationRatios.RevenueGrowth.ThreeMonths, x[0][1].a.OperationRatios.RevenueGrowth.OneYear, x[0][1].a.ValuationRatios.EVToEBITDA, 
                        x[0][1].a.EarningReports.BasicEPS.TwelveMonths, x[0][1].a.ValuationRatios.PERatio,
                        x[0][1].a.FinancialStatements.IncomeStatement.NetIncome.ThreeMonths/1e6, x[0][1].a.OperationRatios.ROIC.Value,  x[0][1].a.OperationRatios.LongTermDebtEquityRatio.Value,
                        x[0][1].a.ValuationRatios.FCFPerShare, x[0][1].a.ValuationRatios.FCFYield,
                        x[0][1].a.EarningReports.NormalizedDilutedEPS.ThreeMonths, x[0][1].a.ValuationRatios.PEGRatio, x[0][1].a.ValuationRatios.FirstYearEstimatedEPSGrowth, 
                        x[0][1].b.price_diff_absolute_slope, x[0][1].b.baseline_slope, x[0][1].b.price_diff_pct_spot, x[0][1].b.price_diff_pct, 
                        x[0][1].b.price_area,
                        x[0][1].b.price_variance, x[0][1].b.price_variance_absolute_slope, x[0][1].b.price_meta_variance_absolute_slope, x[0][1].b.price_variance_above_line,
                        x[0][1].b.pvt_diff_absolute_slope, x[0][1].b.pvt_diff_pct, x[0][1].b.pvt_diff_pct_spot,
                        x[0][1].b.tenkan_kijun_above_kumo, x[0][1].b.kumo_is_green)
                self.algorithm.Log(message)

        
        # diagnostics
        #for x in fine_symbols:
            # if x[0].Value == 'GOOG' or x[0].Value == 'MRNA' or x[0].Value == 'NVDA':
            #     self.algorithm.Log('** SINGLE SYMBOL DIAGNOSTIC **')
            #     message = '{}  3 yr Growth for EVT/EBITDA: {:.2f}   RevenueGrowth: {:.2f}    Free Cash Flow: {:.2f}    marketcap: {:.2f}B    ' \
            #               'price diff slope: {:.2f}   pvt diff slope: {:.2f}   baseline slope: {:.2f}  trend slope: {:.2f}  ' \
            #               'tenkan_kijun_above_kumo {}  price_above_supertrend {}' \
            #         .format(x[0].Value, x[1].a.OperationRatios.RevenueGrowth.ThreeMonths, x[1].a.ValuationRatios.FirstYearEstimatedEPSGrowth, x[1].a.ValuationRatios.FCFPerShare, x[1].a.MarketCap/1e9,
            #             x[1].b.price_diff_absolute_slope, x[1].b.pvt_diff_absolute_slope, x[1].b.baseline_slope, x[1].b.trend_absolute_slope,
            #             x[1].b.tenkan_kijun_above_kumo, x[1].b.price_above_supertrend)
            #     self.algorithm.Log(message)

            # csv diagnostic
            # msg = "log,_fine,symbol,price_diff_abs_slope,price_diff_slope,price_fast_slope,price_slow_slope," \
            #       "pvt_diff_abs_slope,fast_abs_slope,slow_abs_slope,trend_abs_slope"
            #msg = f',_fine,{x[0].Value},{x[1].b.price_diff_absolute_slope:.4f},{x[1].b.price_diff_slope:.4f},' \
            #      f'{x[1].b.price_fast_slope:.4f},{x[1].b.price_slow_slope:.4f},{x[1].b.pvt_diff_absolute_slope:.4f},' \
            #      f'{x[1].b.fast_absolute_slope:.4f},{x[1].b.slow_absolute_slope:.4f},{x[1].b.trend_absolute_slope:.4f}'
            #self.algorithm.Log(msg)


        '''
        
        # this section has way less impact than I thought it woud. Turns out there are very few stocks in fine_symbols that are 'active' 
 
        active_list = []
        renew_list  = []
        new_list    = []
        
        for x in fine_symbols:
            metric_data = self.algorithm.portfolio_metrics.metricsBySymbol.get(x[0])
            if metric_data is not None:
                period_data = metric_data.metricsByPeriod.get(self.algorithm.previous_period)
                if period_data.state == 'active' and period_data.pct_gain_3p >= 0.0:
                    active_list.append(x[0])
                elif period_data.state == 'renew' and period_data.pct_gain_3p >= 0.0:
                    renew_list.append(x[0])
                else:
                    new_list.append(x[0])
            else:
                new_list.append(x[0])
                        

        fine_list = renew_list + active_list + new_list

        
        # diagnostics
        printSymbolList(self.algorithm, "*** Active list", active_list)
        printSymbolList(self.algorithm, "*** Renew list", renew_list)
        printSymbolList(self.algorithm, "*** New list", new_list)
        printSymbolList(self.algorithm, "*** Fine list", fine_list)
        
        '''
                        

        # apply the previous period performance data to find previous stocks to keep
        # if performance data doesn't exist keep the stock for next round
        """
        keep_symbols = dict()
        additional_keep_symbols = dict()
        for symbol, portfolio in self.algorithm.portfolio_metrics.metricsBySymbol.items():
            if symbol not in fine_list:
                previous_period_metrics = portfolio.metricsByPeriod.get(self.algorithm.previous_period)
                if previous_period_metrics is not None:
                    if previous_period_metrics.state == 'keep':
                        keep_symbols[symbol] = previous_period_metrics
                    elif previous_period_metrics.state == 'new'    and (previous_period_metrics.pct_gain_1p >= -0.02):
                        keep_symbols[symbol] = previous_period_metrics
                    elif previous_period_metrics.state == 'active' and (previous_period_metrics.pct_gain_1p >= -0.05):
                        keep_symbols[symbol] = previous_period_metrics
                    elif previous_period_metrics.state == 'renew'  and (previous_period_metrics.pct_gain_1p >= -0.02):
                        keep_symbols[symbol] = previous_period_metrics
                    #elif previous_period_metrics.invested:
                    #    keep_symbols[symbol] = previous_period_metrics
            else:
                previous_period_metrics = portfolio.metricsByPeriod.get(self.algorithm.previous_period)
                if previous_period_metrics is not None:
                    if previous_period_metrics.state == 'keep':
                        additional_keep_symbols[symbol] = previous_period_metrics
                    elif previous_period_metrics.state == 'new'    and (previous_period_metrics.pct_gain_3p >= -0.05):
                        additional_keep_symbols[symbol] = previous_period_metrics
                    elif previous_period_metrics.state == 'active' and (previous_period_metrics.pct_gain_3p >= -0.05):
                        additional_keep_symbols[symbol] = previous_period_metrics
                    elif previous_period_metrics.state == 'renew'  and (previous_period_metrics.pct_gain_3p >= -0.05):
                        additional_keep_symbols[symbol] = previous_period_metrics
                    elif previous_period_metrics.invested:
                        additional_keep_symbols[symbol] = previous_period_metrics

        # add coarse data
        enhanced_keep_symbols = dict()
        for symbol, keep_data in keep_symbols.items():
            enhanced_keep_symbols[symbol] = joindata(
                # get current fundamental data
                keep_data,
                # get current coarse data
                self.algorithm.coarseDataBySymbol[symbol]
            )
            
        enhanced_additional_keep_symbols = dict()
        for symbol, keep_data in additional_keep_symbols.items():
            enhanced_additional_keep_symbols[symbol] = joindata(
                # get current fundamental data
                keep_data,
                # get current coarse data
                self.algorithm.coarseDataBySymbol[symbol]
            )
            
        #diagnostics on keep
        enhanced_keep_symbol_list = [x.Value for x in enhanced_keep_symbols.keys()]
        #self.algorithm.Log(f'** enhanced keep symbols: {enhanced_keep_symbol_list}')

        enhanced_additional_keep_symbol_list = [x.Value for x in enhanced_additional_keep_symbols.keys()]
        #self.algorithm.Log(f'** additional enhanced keep symbols: {enhanced_additional_keep_symbol_list}')

        # filter out symbols that should not be in keep
        filtered_keep_symbols = dict(filter(lambda kv: (kv[1].b.is_uptrend), enhanced_keep_symbols.items()))
        logging_list = []
        for x in filtered_keep_symbols.keys():
            logging_list.append(x)
        #printSymbolList(self.algorithm, "*** filtered keep symbols", logging_list, False)
        
        filtered_additional_keep_symbols = dict(filter(lambda kv: (kv[1].b.is_uptrend), enhanced_additional_keep_symbols.items()))
        logging_list = []
        for x in filtered_additional_keep_symbols.keys():
            logging_list.append(x)
        #printSymbolList(self.algorithm, "*** filtered additional keep symbols", logging_list, False)
        
        # order priority        
        keep_symbols = sorted(filtered_keep_symbols.items(), key=lambda kv: (kv[1].b.tenkan_kijun_above_kumo, kv[1].a.pct_gain_3p), reverse= True) 
        keep_list = [x[0] for x in keep_symbols]
        #printSymbolList(self.algorithm, "*** sorted keep symbols", keep_list, False)
        #self.algorithm.Log(f'** sorted keep symbols: {keep_list}')
        
        additional_keep_symbols = sorted(filtered_additional_keep_symbols.items(), key=lambda kv: (kv[1].b.tenkan_kijun_above_kumo, kv[1].a.pct_gain_3p), reverse= True) 
        additional_keep_list = [x[0] for x in additional_keep_symbols]
        #printSymbolList(self.algorithm, "*** sorted additional keep symbols", additional_keep_list, False)

        # get currently invested stocks
        invested_list = [s.Value.Symbol for s in self.algorithm.ActiveSecurities
                         if s.Value.Invested]

        # manage proper ratio of fine_list to keep_list
        total_size = len(fine_list) + len(keep_list)
        if total_size > self.algorithm.max_universe_size:
            max_keep_size = math.floor(self.algorithm.max_universe_size * self.algorithm.keep_percent)
            keep_list = keep_list[:max_keep_size]
            max_fine_size = self.algorithm.max_universe_size - len(keep_list)
            fine_list = fine_list[:max_fine_size]

        # check to see if any of the keep stocks didn't make it onto the shortened fine list
        remainder_keep_list = [x for x in additional_keep_list if x not in fine_list]
        if len(remainder_keep_list) > 0:
            aggregated_keep_symbols = {**enhanced_additional_keep_symbols, **enhanced_keep_symbols}
            keep_symbols = sorted(aggregated_keep_symbols.items(), key=lambda kv: (kv[1].b.tenkan_kijun_above_kumo, kv[1].a.pct_gain_3p), reverse=True) 
            keep_list = [x[0] for x in keep_symbols]

            # re-do, manage proper ratio of fine_list to keep_list now that keep list is longer
            total_size = len(fine_list) + len(keep_list)
            if total_size > self.algorithm.max_universe_size:
                max_keep_size = math.floor(self.algorithm.max_universe_size * self.algorithm.keep_percent)
                keep_list = keep_list[:max_keep_size]
                max_fine_size = self.algorithm.max_universe_size - len(keep_list)
                fine_list = fine_list[:max_fine_size]

        # Report on resulting universe
        #printSymbolList(self.algorithm, "* Fine new selection", fine_list)
        #printSymbolList(self.algorithm, "* Fine keep from previous", keep_list)
        #printSymbolList(self.algorithm, "* Fine keep Invested ", invested_list)

        # optionally add invested_list to keep_list and log inclusion
        if self.include_invested_in_keep:
            keep_list = list(set(keep_list).union(set(invested_list)))
        printSymbolList(self.algorithm, f'* Fine include invested [{self.include_invested_in_keep}]',
                        list(set(invested_list) - set(keep_list)))

        # Setup PortfolioMetrics objects for the new lists
        # note: Required here because Universe may not change and OnSecuritiesChanged won't be called
        self.algorithm.portfolio_metrics.refresh_metrics_objects(fine_list, keep_list)
        # update allocation
        self.algorithm.allocation_model.update(fine_list, keep_list)

        self.algorithm.rebalance_flag = False  # reset rebalance flag

        result_list = list(set(fine_list).union(set(keep_list)))
        """
        result_list = list(fine_list)
        result_list.sort()

        #Calculating price change in stock and printing it out
        self.algorithm.selected_stocks.append(result_list)
        self.algorithm.monthly_data.append(list())
        self.algorithm.monthly_percent_data.append(list())

        #Initialize new lines to charts
        self.algorithm.chart.AddSeries(Series(SeriesType.Line, name="{}".format(len(self.algorithm.monthly_data))))             #Add a line to Monthly Data Chart
        self.algorithm.percent_chart.AddSeries(Series(SeriesType.Line, name="{}".format(len(self.algorithm.monthly_data))))     #Add a line to Monthly Percent Change Chart

        spy_price = self.algorithm.History(self.algorithm.spy, 1, Resolution.Daily)['close'][0]

        #Plotting Spy
        self.algorithm.Plot("Monthly Data", "SPY", spy_price) #Plot price for spy
        self.algorithm.Plot("Monthly Percent Change", "SPY", spy_price / self.algorithm.spy_intial_price)
        self.algorithm.spy_percent.append(spy_price / self.algorithm.spy_intial_price)

        for i, port in enumerate(self.algorithm.selected_stocks):   #iterate over all stocks chosen over all months
            tempSum = 0
            for stock in port:  #Sum price of all stocks in each month
                price = self.algorithm.History(stock, 1, Resolution.Daily)['close'][0]
                tempSum += price

            self.algorithm.monthly_data[i].append(tempSum)  #Add sum of each monthly portfolio to monthly data
            
            #Plot using monthly_data
            self.algorithm.Plot("Monthly Data", str(i), tempSum)    #Plot monthly data
            if self.algorithm.monthly_data[i][0] > 0:   #check for no divide by 0 errors
                per_change = tempSum / self.algorithm.monthly_data[i][0]
                self.algorithm.Plot("Monthly Percent Change", str(i), per_change)
                self.algorithm.monthly_percent_data[i].append(per_change)
            else:
                self.algorithm.monthly_percent_data[i].append(None)

        #Log data
        self.algorithm.Log("Stock Price by Month:")
        for i, port in enumerate(self.algorithm.selected_stocks):
            printSymbolList(self.algorithm, "Stocks: ", port)
            self.algorithm.Log(self.algorithm.monthly_data[i])

        printSymbolList(self.algorithm, "* Fine result_list", result_list)
        return result_list
                
class joindata:
    def __init__(self, a, b):
        self.a = a      # period performance metrics
        self.b = b      # current coarse data metrics
