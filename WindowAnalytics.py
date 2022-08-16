#region imports
from AlgorithmImports import *
#endregion
# WindowAnalytics

import math
import numpy as np
from scipy.stats import linregress

class Scale:
    '''
    Find max/min of a signal value over a window
    <Scale>.scale is used internally to scale the related analytic functions
    '''

    def __init__(self, parent, delta=False):
        self.parent = parent  # a handle to the parent algorithm context
        self.delta = delta      # True to use (current - previous) as value
        # setup min/max analysis windows
        min_max_window_size = 90
        self._window = RollingWindow[float](min_max_window_size)
        self.value_scale = 1.0
        self._max = -1.0e100
        self._min = 1.0e100
        self._value = 0.0
        self._previous_value = None

    def update(self, value):
        self._value = value
        # handle initial condition
        if self._previous_value == None:
            self._previous_value = self._value
        # compute scale factor using min/max history
        if self.delta:
            self._window.Add(self._value - self._previous_value)
        else:
            self._window.Add(self._value)
        self._previous_value = self._value
        if not self._window.IsReady:
            return

        # find min/max in current window
        # TODO: rewrite for efficiency sometime
        self._max = -1.0e100
        self._min = 1.0e100
        count = self._window.Count
        for i in range(0, count):
            x = self._window[i]
            self._max = max(x, self._max)
            self._min = min(x, self._min)
        _scale = max(abs(self._max), abs(self._min))
        if _scale > 0:
            self.value_scale = 1.0 / _scale
        return

    @property
    def scale(self):
        return self.value_scale

    # these are for debugging
    @property
    def max(self):
        return self._max

    @property
    def min(self):
        return self._min

class LineDiff:
    def __init__(self, parent, _scale, tolerance=0.005):
        '''
        param: parent -- reference to parent algorithm context 
        param: _scale -- reference to Scale object
        param: tolerance (optional) -- magnitude to determine parallel
        '''
        self.parent = parent    # a handle to the parent algorithm context
        self._scale = _scale
        self.tolerance = tolerance
        self._diff = 0.0
        self._current_magnitude = 0.0
        self._previous_magnitude = 0.0   # use for tracking "rate of change"

    def update(self, line1, line2):
        '''
        update latest data point for l1 and l2
        and compute difference magnitude
        '''
        self._diff = line1 - line2
        self._previous_magnitude = self._current_magnitude
        self._current_magnitude = self._diff * self._scale.scale
        return
   

    @property
    def magnitude(self):
        return self._current_magnitude
        
    @property
    def previous_magnitude(self):
        return self._previous_magnitude

    @property
    def magnitude_change(self):
        if self._current_magnitude >= 0.0:
            change = self._current_magnitude - self._previous_magnitude
        else:
            change = self._previous_magnitude - self._current_magnitude
        return change
        
    @property
    def a_above_b(self):
        return self._current_magnitude > self.tolerance
        
    @property
    def a_below_b(self):
        return self._current_magnitude < self.tolerance

    @property
    def a_on_b(self):
        return abs(self._current_magnitude) < self.tolerance

    @property
    def b_above_a(self):
        return self._current_magnitude < self.tolerance
        
    @property
    def b_below_a(self):
        return self._current_magnitude > self.tolerance

    @property
    def parallel(self):
        return abs(self._current_magnitude - self._previous_magnitude) < self.tolerance
        
    @property
    def crossing(self):
        return np.sign(self._current_magnitude) != np.sign(self._previous_magnitude)
    
    @property
    def converging(self):
        if self.parallel:
            result = False
        elif self.crossing:
            result = False
        else:
            result = abs(self._current_magnitude) < abs(self._previous_magnitude)
        return result

    @property
    def diverging(self):
        if self.parallel:
            result = False
        elif self.crossing:
            result = True
        else:
            result = abs(self._current_magnitude) > abs(self._previous_magnitude)
        return result

    # these are for debugging
    @property
    def diff(self):
        return self._diff

    @property
    def max(self):
        return self._scale._max

    @property
    def min(self):
        return self._scale._min


class Slope:
    def __init__(self, parent, _scale):
        '''
        param: parent -- reference to parent algorithm context 
        param: _scale -- reference to Scale object
        '''
        self.parent = parent    # a handle to the parent algorithm context
        self._scale = _scale
        self._value = 0.0
        self._previous_value = None
        self._current_magnitude = 0.0
        self._previous_magnitude = 0.0   # use for tracking "rate of change"

    def update(self, _value):
        self._value = _value
        self._previous_magnitude = self._current_magnitude
        # handle initial condition
        if self._previous_value == None:
            self._previous_value = self._value
        _diff = self._value - self._previous_value
        
        # force 1.0 ~= 90 degrees instead of 45 degrees
        self._current_magnitude = math.atan(_diff * self._scale.scale * math.pi / 2.0)
        self._previous_value = self._value

    @property
    def magnitude(self):
        return self._current_magnitude
        
    @property
    def previous_magnitude(self):
        return self._previous_magnitude

    @property
    def magnitude_change(self):
        if self._current_magnitude >= 0.0:
            change = self._current_magnitude - self._previous_magnitude
        else:
            change = self._previous_magnitude - self._current_magnitude
        return change 

    # these are for debugging
    @property
    def max(self):
        return self._scale._max

    @property
    def min(self):
        return self._scale._min

    @property
    def scale(self):
        return self._scale.scale

def SMA_signal(window):
    """
    Calculate SMA over given RollingWindow
    """
    if window.Count < 1:
        return 0.0
    sum = 0
    for x in window:
        sum = sum + x
    signal = sum/window.Count
    return signal


def WMA_signal(window):
    """
    Calculate WMA value over given RollingWindow
    """
    if window.Count < 1:
        return 0.0
    sum = 0.0
    count = window.Count
    denominator = (count * (count + 1)) / 2
    for x in window:
        sum = sum + (x * count)
        count -= 1
    signal = sum / denominator
    return signal

def window_slope(parent, window):
    '''
    :param parent: caller passes in reference to support logging and symbol
    :param window: RollingWindow
    Calculate slope across the given RollingWindow using linear regression (x, y)
    y = scaled values from the window
    x = a linear x axis created internally
    :return normalized slope [1, -1] or 'nan' if not computable
    '''
    if window.IsReady:
        # create x-axis in reverse order (window[0] is latest)
        x = []
        for i in range(window.Count, 0, -1):
            x.append(i - 1)

        # convert input window to an array
        window_array = [y for y in window]
        avg_value = np.average(np.abs(window_array))   #added Oct4,2021 to enable scaling of slopes with negative values
        try:
            # scale y axis to get reasonable slope
            y_log10 = int(math.log10(avg_value)) - 1
            y_div = pow(10.0, y_log10)
            y = [y / y_div for y in window_array]
        except ValueError as e:
            # ValueError for Log10 of 0 or negative number
            # parent.algorithm.Log(f"Exception: window_slope() {e}: Log10({avg_value}). Symbol: {parent.symbol.Value}")
            return float('nan')

        try:
            # compute linear regression slope
            slope = linregress(x, y)[0]
            angle_radians = math.atan(slope)  # angle in radians
            result = angle_radians / (np.pi / 2)  # normalize to [-1, 1]
            # result = result * slope_sign
        except ValueError as e:
            # linregress len(x) must equal len(y)
            parent.algorithm.Log(f"Exception: window_slope() {e}: linregress(x:{len(x)}, y:{len(y)})."
                                 f" Symbol: {parent.symbol.Value}")
            result = float('nan')
        return result
    else:
        return float('nan')

def relative_area(period, line1, line2):
    """
    Use percent difference (line1-line2)/line2 to compute area over window period.
    If line1 is over line2 result is positive otherwise result is negative.
    If line windows do not contain #period samples then result is 'nan'
    :param period: number of samples to use out of the window
    :param line1: rolling window containing line1
    :param line2: rolling window containing line2
    :return: relative percent area between the lines
    """
    # check window sizes
    if line1.Count < period or line2.Count < period:
        return float('nan')

    pct_diff_list = []
    for i in range(0, period - 1):
        pct_diff = (line1[i] - line2[i]) / line2[i]
        pct_diff_list.append(pct_diff)

    result = np.average(pct_diff_list)
    return result
