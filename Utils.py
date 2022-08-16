#region imports
from AlgorithmImports import *
#endregion
# Utils

import math
import copy


def addDataIndicator(parent, indicator_name, class_name, algorithm, symbol, **kwargs):
    # add new Data Indicator and backfill history
    # addDataIndicator(symbol, indicator_name, indicatorData_class, **kwargs)
    #   where kwargs are name/value of parameters required by indicatorData()
    # algorithm.Log("kwargs: " + str(kwargs))

    # create data indicator instance
    kwargs['algorithm'] = algorithm
    kwargs['symbol'] = symbol
    myInstance = class_name(**kwargs)
    if symbol in algorithm.IndicatorDataBySymbol:
        indicator_dictionary = algorithm.IndicatorDataBySymbol[symbol]
    else:
        indicator_dictionary = dict()
        algorithm.IndicatorDataBySymbol[symbol] = indicator_dictionary
    # add indicator to dictionary
    indicator_dictionary[indicator_name] = myInstance
    return myInstance


def getDataIndicator(self, symbol, indicator_name):
    indicator_dictionary = self.algorithm.IndicatorDataBySymbol[symbol]
    return indicator_dictionary[indicator_name]

def printSymbolList(algorithm, comment, symbols, sortlist=True):
    symbol_list = ""
    temp_symbols = copy.copy(symbols)
    if sortlist:
        temp_symbols.sort()
    for symbol in temp_symbols:
        if symbol is not None:
            symbol_list += symbol.Value + ' '
    message = '{} {}: {}'.format(comment, len(symbols), symbol_list)
    algorithm.Log(message)

def direction_to_string(direction):
    enumText = "FLAT"
    if direction == InsightDirection.Up:
        enumText = "UP"
    elif direction == InsightDirection.Down:
        enumText = "DOWN"
    return enumText


def logit(parent, symbol, direction):
    # log it if changing direction
    # (not foolproof because no visibility to volatility & drawdown trades)
    logit = (not parent.algorithm.Securities[symbol].Invested and \
             direction == InsightDirection.Up) or \
            (parent.algorithm.Securities[symbol].Invested and \
             direction == InsightDirection.Down)
    return logit

    
# emit insight for the given symbol in symbolData
def emit_insight(parent, firing, insights, time, price, comment):
    # firing - reference to a Firing object
    # insights - is the insight array that will be updated
    # time, price, comment - used for log messages
    # Insight(symbol, timedelta, type, direction, magnitude=None, confidence=None, sourceModel=None)
    # insight = Insight.Price("IBM", timedelta(minutes = 20), InsightDirection.Up, None, None, None, 0.25)
    # use base Insight constructor
    
    # suppress insights during certain times
    if parent.algorithm.suppress_insights:
        return
    
    insights.append(Insight(
        firing.symbolData.symbol, parent.predictionInterval,
        InsightType.Price,
        firing.direction,
        firing.magnitude,
        firing.confidence,
        "Oita",  # Source Model
        firing.weight))

    firing.symbolData.previous_direction = firing.direction


def emit_pair_insight(parent, firing, insights, time, price, comment):
    # emit 2 mirrored insights - the opposite pair will recieve the opposite insight
    # firing - reference to a Firing object
    # insights - is the insight array that will be updated
    # time, price, comment - used for log messages

    emit_insight(parent, firing, insights, time, price, comment)

    # take opposite action on the mirror pair
    pairFiring = Firing(firing.symbolData, firing.direction, time, price, comment,
                        confidence=firing.confidence, magnitude=firing.magnitude)
    if firing.symbolData.symbol == parent.long_symbol:
        pairFiring.symbolData = parent.SymbolDataBySymbol[parent.short_symbol]
    elif firing.symbolData.symbol == parent.short_symbol:
        pairFiring.symbolData = parent.SymbolDataBySymbol[parent.long_symbol]
    if firing.direction == InsightDirection.Up:
        pairFiring.direction = InsightDirection.Down
    elif firing.direction == InsightDirection.Down:
        pairFiring.direction = InsightDirection.Up
    emit_insight(parent, pairFiring, insights, time, price, comment)


'''
fire(self, symbolData, direction, confidence, weight, description)
* symbolData includes symbol
    * Can this be derived directly from self? No
    * Self is used to get handle on algorithm
* Provide indicator time and symbol current price for logging
        * Set time, price once up front based on Ichimoku indicator data
* Provide prediction interval and sourceModel in resolution

fire() adds to a list of firings maintained in firing order
state() adds to a list of states

resolve_firings()
Print all firings and states in firing order
Only emit an insight if a firing exists:
If all have the same direction, emit it
If multiple directions, emit Flat
'''

# short form direction
_up = InsightDirection.Up
_down = InsightDirection.Down
_flat = InsightDirection.Flat

_firings = []  # maintain a list of all rules that fired
_states = []
_msg_buffer = []

def init_firings():
    _states.clear()
    _firings.clear()
    _msg_buffer.clear()

class Firing:
    def __init__(self, symbolData, direction, time, price, description, \
                 confidence=0.0, magnitude=0.0, weight=0.0):
        self.symbolData = symbolData
        self.direction = direction
        self.time = time
        self.price = price
        self.description = description
        self.confidence = confidence
        self.magnitude = magnitude
        self.weight = weight


class State(Firing):
    pass


# Handle firing logging
class Logger:
    def __init__(self, algorithm):
        self.algorithm = algorithm
        self.messagesBySymbol = dict()   # holds message List for given symbol

    def add(self, symbol, message):
        # if symbol doesn't exist then create it
        if self.messagesBySymbol.get(symbol) == None:
            self.messagesBySymbol.update({symbol:[]})
        msg_list = self.messagesBySymbol.get(symbol, [])
        msg_list.append(message)
        return
    
    def clear(self, symbol):
        if self.messagesBySymbol.get(symbol) != None:
            self.messagesBySymbol.pop(symbol)
        return
        
    def output(self, symbol):
        msgs = self.messagesBySymbol.get(symbol, [])
        for msg in msgs:
            self.algorithm.Log(msg)
        self.clear(symbol)
        return

    
# fire() for rules that will emit an insight
def fire(parent, symbolData, direction, _insights, _time, _price, comment, \
         confidence=0.0, magnitude=0.0, weight=0.0):
    x = Firing(symbolData, direction, _time, _price, comment, confidence, magnitude, weight)
    _firings.append(x)


# state() for rules that record state but do not emit an insight
def state(parent, symbolData, direction, _insights, _time, _price, comment, \
          confidence=0.0, magnitude=0.0, weight=0.0):
    x = State(symbolData, direction, _time, _price, comment, confidence, magnitude, weight)
    _states.append(x)


def resolve_firings(parent, insights):
    # if detail logging = True then log everything for the specified interval
    detail_logging = parent.algorithm.log_detail_insights
    detail_start = parent.algorithm.detail_start
    detail_end = parent.algorithm.detail_end
    current_date = parent.algorithm.Time.strftime('%Y-%m-%d')
    
    # Get aggregate bullish and bearish confidence
    final_insight = None
    bullishConfidence = 0.0
    bearishConfidence = 0.0
    joined_list = _firings + _states
    fire_number = 0
    for f in joined_list:
        if not final_insight:
            final_insight = f  # grab 1st rule to populate final insight fields - why?
        fire_number += 1
        if f.direction > 0:
            bullishConfidence += f.confidence
        elif f.direction < 0:
            bearishConfidence += f.confidence
        # logging info
        directionText = direction_to_string(f.direction)
        if type(f) is Firing:
            ftype = "fire"
        elif type(f) is State:
            ftype = "state"
        else:
            ftype = "unknown"
        if parent.algorithm.log_insights:
            # buffer final insight for output later
            message = '{} #{} {} {} {} c: {:.2f} m: {:.2f} w: {:.2f}'. \
                format(ftype, fire_number, f.symbolData.symbol.Value, directionText, f.description,
                      f.confidence, f.magnitude, f.weight)
            parent.algorithm.logger.add(f.symbolData.symbol, message)

    # emit insight if had a firing
    if len(_firings) > 0:
        # normalize confidence
        if bullishConfidence >= 0.0:
            bullishConfidence = min(bullishConfidence, 1.0)
        else:
            bullishConfidence = 0.0
        if bearishConfidence >= 0.0:
            bearishConfidence = min(bearishConfidence, 1.0)
        else:
            bearishConfidence = 0.0
        if bullishConfidence > bearishConfidence:
            final_insight.confidence = max(bullishConfidence - bearishConfidence, 0)
            final_insight.direction = _up
        elif bullishConfidence < bearishConfidence:
            final_insight.confidence = max(bearishConfidence - bullishConfidence, 0)
            final_insight.direction = _down
        emit_insight(parent, final_insight, insights,
                          final_insight.time, final_insight.price, final_insight.description)

    # write detail log if required
    f = final_insight
    if detail_logging and detail_start <= current_date <= detail_end:
        # output final insight
        enumText = direction_to_string(f.direction)
        message = 'final_insight: {} {} c: {:.2f} m: {:.2f} w: {:.2f}'. \
            format(f.symbolData.symbol.Value, enumText, f.confidence, f.magnitude, f.weight)
        parent.algorithm.logger.add(f.symbolData.symbol, message)
        # output cached log lines
        parent.algorithm.logger.output(f.symbolData.symbol)
    
    # write log if have firings and changing direction
    elif len(_firings) > 0 and logit(parent, f.symbolData.symbol, final_insight.direction):
        # output final insight
        enumText = direction_to_string(f.direction)
        message = 'final_insight: {} {} c: {:.2f} m: {:.2f} w: {:.2f}'. \
            format(f.symbolData.symbol.Value, enumText, f.confidence, f.magnitude, f.weight)
        parent.algorithm.logger.add(f.symbolData.symbol, message)
        if parent.algorithm.log_insights:
            # output standard logging
            parent.algorithm.logger.output(f.symbolData.symbol)

    # clear for next time
    init_firings()
    return
