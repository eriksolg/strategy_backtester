import json
import math
import os
import pickle
from datetime import datetime, timedelta
import pandas as pd
import random
from enum import Enum

#Constants
HISTORY_FILE = "MES_1min_continuous_adjusted.txt"
POSITION_FILE = "positions.csv"
CACHE_FILE = 'backtest_sessions_cache.pkl'
TICK_SIZE = 0.25
VALUE_OF_TICK = 1.25
ENTRY_PORTFOLIO = 8000
MAINTENANCE_MARGIN = 0.25
MAX_PORTFOLIO_LOSS_PER_TRADE = 0.06
THREE_HOUR_BREAKEVEN = True
RETRY_DISALLOWED = []
DISABLED_STRATEGIES = []
EXIT_FINAL = "16:00:00"
STRATEGY_SETTINGS = {
    "pivot": {
        "break_even_atr": 8,
        "take_profit_atr": 16,
        "last_enter": "15:54:00"
    },
    "rsi": {
        "break_even_atr": 11,
        "take_profit_atr": 8.5,
        "last_enter": "13:30:00"
    },
    "ret": {
        "break_even_atr": 1.5,
        "take_profit_atr": 12,
        "last_enter": "15:30:00"
    },
    "brk": {
        "break_even_atr": 2,
        "take_profit_atr": 5,
        "last_enter": "14:30:00"
    }
}
STRATEGY_SETTINGS["rsic"] = STRATEGY_SETTINGS["rsi"]
STRATEGY_SETTINGS["rsis"] = STRATEGY_SETTINGS["rsi"]
STRATEGY_SETTINGS["rets"] = STRATEGY_SETTINGS["ret"]
STRATEGY_SETTINGS["reth"] = STRATEGY_SETTINGS["ret"]
STRATEGY_SETTINGS["rethu"] = STRATEGY_SETTINGS["ret"]
STRATEGY_SETTINGS["retu"] = STRATEGY_SETTINGS["ret"]
STRATEGY_SETTINGS["retw"] = STRATEGY_SETTINGS["ret"]
STRATEGY_SETTINGS["retwu"] = STRATEGY_SETTINGS["ret"]
STRATEGY_SETTINGS["retwhu"] = STRATEGY_SETTINGS["ret"]
STRATEGY_SETTINGS["retws"] = STRATEGY_SETTINGS["ret"]
STRATEGY_SETTINGS["retwh"] = STRATEGY_SETTINGS["ret"]



class PositionType(Enum):
    LONG = 0
    SHORT = 1

class PositionStatus(Enum):
    OPEN = 0
    CLOSED = 1
    WAITING = 2
    DISCARDED = 3

class CandleDirection(Enum):
    BULL = 0
    BEAR = 1
    NEUTRAL = 2

class Candle:
    def __init__(self, timestamp, open_price, close_price, low_price, high_price, volume):
        self.timestamp = timestamp
        self.open = open_price
        self.close = close_price
        self.low_price = low_price
        self.high_price = high_price
        self.volume = volume
        self.direction = self.__calculate_direction()
        self.delta = self.__calculate_delta()
        self.distance_to_high = abs(self.open - self.high_price)
        self.distance_to_low = abs(self.open - self.low_price)

    def __calculate_direction(self):
        if self.close > self.open:
            return CandleDirection.BULL
        if self.close < self.open:
            return CandleDirection.BEAR
        return CandleDirection.NEUTRAL

    def __calculate_delta(self):
        return abs(self.low_price - self.high_price)

    def __str__(self):
        return str(self.timestamp)

    def __repr__(self):
        return self.__str__()


class Position:
    def __init__(self, position_type, timestamp, atr, take_profit, stop_loss_price, strategy):
        self.position_type = position_type
        self.timestamp = self.last_timestamp = timestamp
        self.status = PositionStatus.WAITING
        self.take_profit = take_profit
        self.exit_final = EXIT_FINAL
        self.atr = atr
        self.stop_loss_price = stop_loss_price
        self.strategy = strategy
        self.break_even = STRATEGY_SETTINGS[self.strategy]["break_even_atr"] * self.atr

        self.entry_price = None
        self.realized_pl = None
        self.unrealized_pl = None
        self.stop_loss = None
        self.position_size = None
        self.last_timestamp = None
        self.portfolio_size_on_open = None

    def open_position(self, entry_price):
        self.portfolio_size_on_open = Backtest.portfolio_size
        self.status = PositionStatus.OPEN
        self.entry_price = entry_price
        self.unrealized_pl = 0
        if math.isnan(self.stop_loss_price):
            self.stop_loss = -2 * self.atr
        else:
            if self.stop_loss_price > 1000:
                self.stop_loss = -1 * abs(self.stop_loss_price-entry_price)
            else:
                self.stop_loss = self.stop_loss_price

        self.initial_stop_loss = self.stop_loss

        position_size = self.calculate_initial_position_size()
        if position_size is not None:
            self.position_size = position_size
            Backtest.portfolio_size = Backtest.portfolio_size - self.entry_price * self.position_size
        else:
            print(f"Cannot open position for ${self}. Not enough margin to cover stop loss.")
            self.position_size = 0
            self.close_position(status = PositionStatus.DISCARDED)
        if abs(self.stop_loss) < self.atr:
            self.close_position(status = PositionStatus.DISCARDED)

    def calculate_initial_position_size(self):
        margin_requirement = self.entry_price * MAINTENANCE_MARGIN
        max_positions = int(Backtest.portfolio_size // margin_requirement)

        for position_size in range(max_positions, 1, -1):
            potential_loss = self.get_value_in_usd(self.stop_loss, position_size)
            if potential_loss >= (-1 * Backtest.portfolio_size * MAX_PORTFOLIO_LOSS_PER_TRADE):
                return position_size
        return None

    def close_position(self, pl = None, status = PositionStatus.CLOSED):
        if self.isClosed():
            return
        if pl is None:
            pl = self.unrealized_pl
        self.status = status
        # Slippage
        # pl = pl - 0.2
        self.realized_pl = self.get_value_in_usd(pl, self.position_size)
        self.unrealized_pl = None
        Backtest.portfolio_size = Backtest.portfolio_size + self.entry_price * self.position_size

    def handle_unrealized_pl(self, candle):
        if self.isClosed():
            return
        if self.position_type == PositionType.LONG:
            self.unrealized_pl = candle.open - self.entry_price
        else:
            self.unrealized_pl = self.entry_price - candle.open

    def get_value_in_usd(self, valueInPoints, position_size):
        return (valueInPoints / TICK_SIZE) * VALUE_OF_TICK * position_size

    def handle_stop_loss(self, candle):
        if self.isClosed():
            return
        if (self.position_type == PositionType.LONG and self.unrealized_pl - candle.distance_to_low <= self.stop_loss) or \
            (self.position_type == PositionType.SHORT and self.unrealized_pl - candle.distance_to_high <= self.stop_loss):
            self.close_position(self.stop_loss)

    def handle_take_profit(self, candle):
        if self.isClosed():
            return
        if (self.unrealized_pl >= STRATEGY_SETTINGS[self.strategy]["take_profit_atr"] * self.atr):
            self.close_position()
            return
        if self.take_profit == "":
            return
        if (candle.timestamp.time() >= (datetime.strptime(self.take_profit, '%H:%M:%S').time())):
            self.close_position()

    def handle_break_even(self, candle):
        if self.isClosed():
            return
        if ((self.position_type == PositionType.LONG and self.unrealized_pl + candle.distance_to_high >= self.break_even) or
            (self.position_type == PositionType.SHORT and self.unrealized_pl + candle.distance_to_low >= self.break_even)) and self.stop_loss<0:
            self.stop_loss = 0.5
        atr_var = 2 * self.atr
        if THREE_HOUR_BREAKEVEN and (self.last_timestamp - self.timestamp >= timedelta(hours=3) and self.unrealized_pl >= atr_var):
            self.stop_loss = atr_var

    def handle_end_of_day(self, candle):
        if self.isClosed():
            return
        if (candle.timestamp.time() >= (datetime.strptime(self.exit_final, '%H:%M:%S').time())):
            self.close_position()
    
    def isOpen(self):
        return self.status is PositionStatus.OPEN

    def isClosed(self):
        return self.status is PositionStatus.CLOSED
    
    def isWaiting(self):
        return self.status is PositionStatus.WAITING

    def isDiscarded(self):
        return self.status is PositionStatus.DISCARDED

    def __str__(self):
        return str(f"Position({self.timestamp})")

    def __repr__(self):
        return self.__str__()


class Session:
    def __init__(self, date, candles):
        self.date = date
        self.candles = candles
        self.positions = []
        self.realized_pl = 0
        self.exceed_monthly_pl = False

    def add_position(self, position):
        self.positions.append(position)

    def position_filter(self, position):
        if position.strategy in DISABLED_STRATEGIES:
            return False
        if len([pos for pos in self.positions if pos.isClosed()]) > 0:
            return False
        # if len([pos for pos in self.positions if pos.isClosed() and pos.strategy == position.strategy]) > 0:
        #     return False
        if len([pos for pos in self.positions if pos is not position and pos.isOpen()]) > 0:
            return False

        if position.timestamp.time() > datetime.strptime(STRATEGY_SETTINGS[position.strategy]["last_enter"], '%H:%M:%S').time():
            return False

        return True

    def run_backtest(self):
        for candle in self.candles:
            for position in self.positions:
                if not self.position_filter(position):
                    continue
                if position.isWaiting():
                    if position.timestamp >= candle.timestamp and position.timestamp < candle.timestamp + timedelta(minutes=1):
                        position.open_position(candle.open)

                if position.isOpen():
                    position.last_timestamp = candle.timestamp
                    position.handle_unrealized_pl(candle)
                    position.handle_stop_loss(candle)
                    position.handle_take_profit(candle)
                    position.handle_break_even(candle)
                    position.handle_end_of_day(candle)
                    self.realized_pl += position.realized_pl if position.isClosed() else 0

    def __str__(self):
        return str(f"Session: {self.date}")

    def __repr__(self):
        return self.__str__()


class Backtest:
    portfolio_size = ENTRY_PORTFOLIO
    def __init__(self):
        self.sessions = []
        self.results = []
        self.monthly_pl = {}
        self.monthly_pl_per_strategy = {}
        self.yearly_pl = {}
        self.monthly_return = {}
        self.yearly_return = {}
        self.win_ratios = {}
        self.risks_per_session = {}
        
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'rb') as f:
                self.sessions = pickle.load(f)

    def has_sessions(self):
        return len(self.sessions) > 0

    def save_sessions_to_cache(self):
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(self.sessions, f)

    def add_session(self, session):
        self.sessions.append(session)

    def find_session(self, date):
        for session in self.sessions:
            if session.date == date:
                return session
        return None

    def run(self):

        for session in self.sessions:
            if len(session.positions) == 0:
                continue
            session.run_backtest()
            Backtest.portfolio_size += session.realized_pl
            

            month_year = session.date.strftime('%Y-%m')
            year = session.date.strftime('%Y')
            if month_year not in self.monthly_pl:
                self.monthly_pl[month_year] = 0
            
            
            self.monthly_pl[month_year] += session.realized_pl

            if month_year not in self.monthly_return:
                self.monthly_return[month_year] = 0
                initial_portfolio_for_month = Backtest.portfolio_size
            self.monthly_return[month_year] = round(self.monthly_pl[month_year] / initial_portfolio_for_month, 4)

            if year not in self.yearly_pl:
                self.yearly_pl[year] = 0
            self.yearly_pl[year] += session.realized_pl

            if year not in self.yearly_return:
                self.yearly_return[year] = 0
                initial_portfolio_for_year = Backtest.portfolio_size
            self.yearly_return[year] = round(self.yearly_pl[year] / initial_portfolio_for_year, 4)

    def __calculate_session_pl(self):
        session_PL = {session.date.strftime('%Y-%m-%d'): session.realized_pl for session in self.sessions if len(session.positions) > 0}
        return session_PL
    
    def __calculate_win_ratio(self):
        losses = []
        wins = []
        for session in self.sessions:
            for position in [pos for pos in session.positions if pos.realized_pl is not None]:
                if position.realized_pl > 0:
                    wins.append(position.realized_pl)
                elif position.realized_pl < 0:
                    losses.append(position.realized_pl)
        return len(wins)/len(losses + wins)
    
    def __calculate_average_return(self):
        return sum(monthlyReturn for month, monthlyReturn in self.monthly_return.items()) / len(self.monthly_return)

    def __calculate_average_no_sessions_per_month(self):
        return len([session for session in self.sessions if len(session.positions) != 0]) / len(self.monthly_return)
    
    def __calculate_average_profit_ratio(self):
        losses = []
        wins = []
        for session in self.sessions:
            for position in [pos for pos in session.positions if pos.realized_pl is not None]:
                if position.realized_pl > 0:
                    wins.append(position.realized_pl)
                elif position.realized_pl < 0:
                    losses.append(position.realized_pl)
        average_loss = sum(losses)/len(losses) if len(losses) else 0
        average_win = sum(wins)/len(wins) if len(wins) else 0
        
        return abs(average_loss / average_win) if average_win else 0

    def __calculate_average_monthly_return_per_year(self):
        yearly_monthly_returns = {}
        for month, return_value in self.monthly_return.items():
            year = month.split('-')[0]
            if year not in yearly_monthly_returns:
                yearly_monthly_returns[year] = []
            yearly_monthly_returns[year].append(return_value)

        yearly_average_returns = {}
        for year, monthly_returns in yearly_monthly_returns.items():
            yearly_average_returns[year] = round(sum(monthly_returns) / len(monthly_returns), 4)

        return yearly_average_returns

    def __calculate_biggest_drawdown(self):
        biggest_drawdown = 0
        current_drawdown = 0
        biggest_drawdown_pos = None
        for session in self.sessions:
            for position in [pos for pos in session.positions if pos.realized_pl is not None]:
                if position.realized_pl < 0:
                    current_drawdown += 1
                    if current_drawdown > biggest_drawdown:
                        biggest_drawdown = current_drawdown
                        biggest_drawdown_pos = position
                else:
                    current_drawdown = 0
        return [biggest_drawdown, biggest_drawdown_pos]

    def __calculate_average_portfolio_risk(self):
        risks = []
        for session in self.sessions:
            for position in [pos for pos in session.positions if pos.realized_pl is not None]:
                risks.append(position.get_value_in_usd(position.initial_stop_loss, position.position_size)/position.portfolio_size_on_open)
        return round(sum(risks) / len(risks), 4)


    def print_results(self):
        print("Yearly PL: ", json.dumps(self.yearly_pl))
        print("Monthly PL: ", json.dumps(self.monthly_pl))
        # print("Session PL: ", json.dumps(self.__calculate_session_pl()))
        print("Monthly return: ", self.monthly_return)
        print("Yearly return: ", self.yearly_return)
        print("Initial Portfolio: ", ENTRY_PORTFOLIO)
        print("Final Portfolio: ", Backtest.portfolio_size)
        print("Win ratio: ", self.__calculate_win_ratio())
        print("Average monthly return: ", self.__calculate_average_return())
        print("Average monthly return per year: ", self.__calculate_average_monthly_return_per_year())
        print("Average no sessions per month: ", self.__calculate_average_no_sessions_per_month())
        print("Average profit ratio: ", self.__calculate_average_profit_ratio())
        print("Biggest drawdown: ", self.__calculate_biggest_drawdown())
        print("Average portfolio risk: ", self.__calculate_average_portfolio_risk())


def main():
    candle_data = pd.read_csv(HISTORY_FILE)
    candle_data_grouped_by_date = candle_data.groupby(candle_data.columns[0])

    position_data = pd.read_csv(POSITION_FILE)
    position_data_grouped_by_date = position_data.groupby(position_data.columns[0])

    bt = Backtest()

    if not bt.has_sessions():
        for date, candles in candle_data_grouped_by_date:
            candle_list = []
            for _, candle in candles.iterrows():
                candle_list.append(
                    Candle(
                        datetime.strptime(f"{candle.date} {candle.time}", "%Y-%m-%d %H:%M:%S"),
                        candle.open,
                        candle.close,
                        candle.low,
                        candle.high,
                        candle.volume
                    )
                )
            bt.add_session(
                Session(
                    datetime.strptime(date, "%Y-%m-%d").date(),
                    candle_list
                )
            )
        bt.save_sessions_to_cache()
    
    for date, positions in position_data_grouped_by_date:
        session = bt.find_session(datetime.strptime(date, "%Y-%m-%d").date())
        if session:
            for _, position in positions.iterrows():
                position_type = PositionType.LONG if position.type == "L" else PositionType.SHORT
                session.add_position(
                    Position(
                        position_type,
                        datetime.strptime(f"{position.date} {position.time}", "%Y-%m-%d %H:%M:%S"),
                        position.atr,
                        position.tp if isinstance(position.tp, str) else "",
                        position.sl,
                        position.strategy
                    )
                )
        else:
            print(f"Session with date {date} not found.")

    bt.run()
    bt.print_results()


if __name__ == "__main__":
    main()
