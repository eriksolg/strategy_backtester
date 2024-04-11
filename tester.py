import json
import math
import os
import pickle
from datetime import datetime, timedelta
import pandas as pd
import random

#Constants
HISTORY_FILE = "MES_1min_continuous_adjusted.txt"
POSITION_FILE = "positions.csv"
CACHE_FILE = 'backtest_sessions_cache.pkl'
TICK_SIZE = 0.25
VALUE_OF_TICK = 1.25
ENTRY_PORTFOLIO = 8000
MAINTENANCE_MARGIN = 0.25
MAX_PORTFOLIO_LOSS_PER_TRADE = 0.06
EXIT_FINAL = "16:00:00"
BREAK_EVEN_ATR = {
    "pivot": 7,
    "rsi": 1,
    "ret": 2,
    "retw": 1,
    "brk": 1
}


class Candle:
    DIRECTION_BULL = 1
    DIRECTION_BEAR = -1
    DIRECTION_NEUTRAL = 0

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
            return self.DIRECTION_BULL
        if self.close < self.open:
            return self.DIRECTION_BEAR
        return self.DIRECTION_NEUTRAL

    def __calculate_delta(self):
        return abs(self.low_price - self.high_price)

    def __str__(self):
        return str(self.timestamp)

    def __repr__(self):
        return self.__str__()


class Position:
    POSITION_LONG = 1
    POSITION_SHORT = 0
    POSITION_OPENED = 1
    POSITION_WAITING = -1
    POSITION_CLOSED = 0
    POSITION_DISCARDED = 2

    def __init__(self, position_type, timestamp, atr, take_profit, stop_loss_price, strategy, vwap, month_vwap):
        self.position_type = position_type
        self.timestamp = self.last_timestamp = timestamp
        self.status = Position.POSITION_WAITING
        self.take_profit = take_profit
        self.exit_final = EXIT_FINAL
        self.atr = atr
        self.stop_loss_price = stop_loss_price
        self.strategy = strategy
        self.break_even = BREAK_EVEN_ATR[self.strategy] * self.atr
        self.vwap = vwap
        self.month_vwap = month_vwap

        self.entry_price = None
        self.realized_pl = None
        self.unrealized_pl = None
        self.stop_loss = None
        self.position_size = None
        self.last_timestamp = None

    def open_position(self, entry_price, portfolio_size, scale_down_ratio):
        self.status = Position.POSITION_OPENED
        self.entry_price = entry_price
        self.unrealized_pl = 0
        if math.isnan(self.stop_loss_price):
            self.stop_loss = -2 * self.atr
        else:
            self.stop_loss = -1 * abs(self.stop_loss_price-entry_price)
        position_size = self.calculate_initial_position_size(portfolio_size, scale_down_ratio)
        # if  abs(self.stop_loss) < 1 * self.atr:
        #     self.position_size = 0
        #     self.close_position(status = Position.POSITION_DISCARDED)
        if position_size is not None:
            self.position_size = position_size
        else:
            print(f"Cannot open position for ${self}. Not enough margin to cover stop loss.")
            self.position_size = 0
            self.close_position(status = Position.POSITION_DISCARDED)

    def calculate_initial_position_size(self, portfolio_size, scale_down_ratio):
        margin_requirement = self.entry_price * MAINTENANCE_MARGIN
        max_positions = int(portfolio_size // margin_requirement)

        for position_size in range(max_positions, 1, -1):
            potential_loss = self.get_value_in_usd(self.stop_loss, position_size)
            if potential_loss >= (-1 * portfolio_size * MAX_PORTFOLIO_LOSS_PER_TRADE):
                size = math.floor(1 * position_size)
                return size if size > 0 else None
        return None

    def close_position(self, pl = None, status = POSITION_CLOSED):
        if pl is None:
            pl = self.unrealized_pl
        self.status = status
        self.realized_pl = self.get_value_in_usd(pl, self.position_size)
        self.unrealized_pl = None

    def handle_unrealized_pl(self, candle):
        if self.position_type == Position.POSITION_LONG:
            self.unrealized_pl = candle.open - self.entry_price
        else:
            self.unrealized_pl = self.entry_price - candle.open

    def get_value_in_usd(self, valueInPoints, position_size):
        return (valueInPoints / TICK_SIZE) * VALUE_OF_TICK * position_size

    def handle_stop_loss(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if (self.position_type == Position.POSITION_LONG and self.unrealized_pl - candle.distance_to_low <= self.stop_loss) or \
            (self.position_type == Position.POSITION_SHORT and self.unrealized_pl - candle.distance_to_high <= self.stop_loss):
            self.close_position(self.stop_loss)

    def handle_take_profit(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if self.take_profit == "":
            return
        # if self.unrealized_pl >= 3 * self.atr and self.strategy == "rsi":
        #     self.close_position()
        if (candle.timestamp.time() >= (datetime.strptime(self.take_profit, '%H:%M:%S').time())):
            self.close_position()

    def handle_break_even(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if ((self.position_type == Position.POSITION_LONG and self.unrealized_pl + candle.distance_to_high >= self.break_even) or
            (self.position_type == Position.POSITION_SHORT and self.unrealized_pl + candle.distance_to_low >= self.break_even)) and self.stop_loss<0:
            self.stop_loss = 0

    def handle_end_of_day(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if (candle.timestamp.time() >= (datetime.strptime(self.exit_final, '%H:%M:%S').time())):
            self.close_position()

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

    def position_filter(self, position, vwap_switch):
        # if vwap_switch:
        #     if (position.position_type == Position.POSITION_LONG and float(position.vwap) < float(position.month_vwap)) or \
        #         (position.position_type == Position.POSITION_SHORT and float(position.vwap) > position.month_vwap):
        #         return False
        # if position.position_type == Position.POSITION_LONG and position.strategy != "rsi":
        #     return False
        # if (position.timestamp.time() >= datetime.strptime("15:00:00", '%H:%M:%S').time()):
        #     return False


        # return False
    

        #     return False
        # First position of the day
        if len([pos for pos in self.positions if pos.status is Position.POSITION_CLOSED]) > 0:
            return False
        if position.strategy == "rsi":
            return False
        if position.strategy == "rsi" and (position.timestamp.time() >= datetime.strptime("15:00:00", '%H:%M:%S').time()):
            return False
        # for posi in [pos for pos in self.positions if pos.status is Position.POSITION_CLOSED and pos.last_timestamp < position.timestamp]:
        #     print(posi)
        #     print(posi.realized_pl)
        # if len([pos for pos in self.positions if pos.status is Position.POSITION_CLOSED and pos.last_timestamp > position.timestamp]) > 0:
        #     return False
        
        # if self.realized_pl > 0:
        #     return False

        return True

    def run_backtest(self, portfolio_size, scale_down_ratio, vwap_switch):
        for position in self.positions:
            if not self.position_filter(position, vwap_switch):
                continue

            self.__run_backtest_for_position(position, portfolio_size, scale_down_ratio)
            self.realized_pl += position.realized_pl if position.status == Position.POSITION_CLOSED else 0
            if position.status == Position.POSITION_WAITING:
                print("Position did not execute: ", position)

    def __run_backtest_for_position(self, position, portfolio_size, scale_down_ratio):
        for candle in self.candles:
            if position.status == Position.POSITION_WAITING:
                if position.timestamp >= candle.timestamp and position.timestamp < candle.timestamp + timedelta(minutes=1):
                    position.open_position(candle.open, portfolio_size, scale_down_ratio)

            if position.status == Position.POSITION_OPENED:
                position.last_timestamp = candle.timestamp
                position.handle_unrealized_pl(candle)
                position.handle_stop_loss(candle)
                position.handle_take_profit(candle)
                position.handle_break_even(candle)
                position.handle_end_of_day(candle)

    def __str__(self):
        return str(f"Session: {self.date}")

    def __repr__(self):
        return self.__str__()


class Backtest:
    def __init__(self):
        self.sessions = []
        self.portfolio_size = ENTRY_PORTFOLIO
        self.results = []
        self.monthly_pl = {}
        self.monthly_pl_per_strategy = {}
        self.yearly_pl = {}
        self.monthly_return = {}
        self.yearly_return = {}
        self.scale_down_ratio = 1
        self.vwap_switch = False
        self.win_ratios = {}
        
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
            session.run_backtest(self.portfolio_size, self.scale_down_ratio, self.vwap_switch)
            self.portfolio_size += session.realized_pl
            

            month_year = session.date.strftime('%Y-%m')
            year = session.date.strftime('%Y')
            if month_year not in self.monthly_pl:
                monthly_values = list(self.monthly_pl.values())
                if len(monthly_values) >= 2:
                    if monthly_values[-1] < 0 and monthly_values[-2] < 0:
                        self.scale_down_ratio = self.scale_down_ratio * 0.5
                        self.vwap_switch = True
                    elif monthly_values[-1] > 0:
                        self.scale_down_ratio = 1
                        self.vwap_switch = False
                self.monthly_pl[month_year] = 0
            
            
            self.monthly_pl[month_year] += session.realized_pl

            if month_year not in self.monthly_return:
                self.monthly_return[month_year] = 0
                initial_portfolio_for_month = self.portfolio_size           
            self.monthly_return[month_year] = round(self.monthly_pl[month_year] / initial_portfolio_for_month, 2)

            if year not in self.yearly_pl:
                self.yearly_pl[year] = 0
            self.yearly_pl[year] += session.realized_pl

            if year not in self.yearly_return:
                self.yearly_return[year] = 0
                initial_portfolio_for_year = self.portfolio_size
            self.yearly_return[year] = round(self.yearly_pl[year] / initial_portfolio_for_year, 2)

    def __calculate_session_pl(self):
        session_PL = {session.date.strftime('%Y-%m-%d'): session.realized_pl for session in self.sessions if len(session.positions) > 0}
        return session_PL
    
    def __calculate_win_ratio(self):
        no_sessions = len([session for session in self.sessions if len(session.positions) != 0])
        win_sessions = len([session for session in self.sessions if session.realized_pl > 0 and len(session.positions) != 0])
        return win_sessions/no_sessions
    
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


    def print_results(self):
        print("Yearly PL: ", json.dumps(self.yearly_pl))
        print("Monthly PL: ", json.dumps(self.monthly_pl))
        print("Session PL: ", json.dumps(self.__calculate_session_pl()))
        print("Monthly return: ", self.monthly_return)
        print("Yearly return: ", self.yearly_return)
        print("Initial Portfolio: ", ENTRY_PORTFOLIO)
        print("Final Portfolio: ", self.portfolio_size)
        print("Win ratio: ", self.__calculate_win_ratio())
        print("Average monthly return: ", self.__calculate_average_return())
        print("Average no sessions per month: ", self.__calculate_average_no_sessions_per_month())
        print("Average profit ratio: ", self.__calculate_average_profit_ratio())


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
                position_type = Position.POSITION_LONG if position.type == "L" else Position.POSITION_SHORT
                session.add_position(
                    Position(
                        position_type,
                        datetime.strptime(f"{position.date} {position.time}", "%Y-%m-%d %H:%M:%S"),
                        position.atr,
                        position.tp if isinstance(position.tp, str) else "",
                        position.sl,
                        position.strategy,
                        position.vwap,
                        position.mvwap
                    )
                )
        else:
            print(f"Session with date {date} not found.")

    bt.run()
    bt.print_results()


if __name__ == "__main__":
    main()
