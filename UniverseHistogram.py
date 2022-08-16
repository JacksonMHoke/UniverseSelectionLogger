#region imports
from AlgorithmImports import *
#endregion
'''
UniverseHistogram
Maintain a histogram of stocks added to a Universe
Includes symbol and month(s) of being added to a Universe
Typically called from FineSelection
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
QCAlgorithmFramework = QCAlgorithm
QCAlgorithmFrameworkBridge = QCAlgorithm

class UniverseHistogram:
    def __init__(self, algorithm):
        self.algorithm = algorithm
        
    # writes the histogram of symbols selected
    def print_histogram(self, portfolio_metrics):
        self.algorithm.Log('-------- Histogram --------')
        # portfolio_metrics[symbol]->MetricData->metricsByPeriod[month]->PeriodMetricData
        histogram_data = dict(sorted(portfolio_metrics.metricsBySymbol.items(), key=lambda kv: kv[0]))
        for symbol, portfolio in histogram_data.items():
            month_chart = ''
            month_data = dict(sorted(portfolio.metricsByPeriod.items(), key=lambda kv: kv[0]))
            for month, data in month_data.items():
                month_number = int(month.split('-')[1])
                year_number = int((month.split('-')[0])[-2:])
                if data.state in ('keep'):
                    month_chart = month_chart + f' {year_number:>2d}.{month_number:>2d}* '
                elif data.state not in ('idle', 'keep'):
                    month_chart = month_chart + f' {year_number:>2d}.{month_number:>2d}  '
            self.algorithm.Log(f' {symbol.Value:<5s} -> {month_chart}')
        self.algorithm.Log('---------------------------')
