import json
import math
import os
import pickle
from datetime import datetime, timedelta
import pandas as pd

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
RSI_TP_3ATR = False
NO_TRADE_AFTER_1430 = False
BREAK_EVEN_ATR = {
    "pivot": 5,
    "rsi": 1,
    "ret": 2,
    "retw": 2,
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

    def __init__(self, position_type, timestamp, atr, take_profit, stop_loss, strategy, vwap, month_vwap):
        self.position_type = position_type
        self.timestamp = timestamp
        self.status = Position.POSITION_WAITING
        self.take_profit_price = take_profit
        self.exit_final = EXIT_FINAL
        self.atr = atr
        self.stop_loss_price = stop_loss
        self.strategy = strategy
        self.break_even = BREAK_EVEN_ATR[self.strategy] * self.atr
        self.vwap = vwap
        self.month_vwap = month_vwap

        self.entry_price = None
        self.realized_pl = None
        self.unrealized_pl = None
        self.take_profit = None
        self.stop_loss = None
        self.position_size = None

    def open_position(self, entry_price, portfolio_size):
        self.status = Position.POSITION_OPENED
        self.entry_price = entry_price
        self.unrealized_pl = 0
        self.take_profit = abs(self.entry_price - self.take_profit_price)
        if RSI_TP_3ATR and self.strategy == "rsi":
            self.take_profit = 3 * self.atr
        if math.isnan(self.stop_loss_price):
            self.stop_loss = -2 * self.atr
        else:
            self.stop_loss = -1 * abs(self.stop_loss_price-entry_price)
        position_size = self.calculate_initial_position_size(portfolio_size)
        if position_size is not None:
            self.position_size = position_size
        else:
            print(f"Cannot open position for ${self}. Not enough margin to cover stop loss.")
            self.position_size = 0
            self.close_position(status = Position.POSITION_DISCARDED)

    def calculate_initial_position_size(self, portfolio_size):
        margin_requirement = self.entry_price * MAINTENANCE_MARGIN
        max_positions = int(portfolio_size // margin_requirement)

        for position_size in range(max_positions, 1, -1):
            potential_loss = self.get_value_in_usd(self.stop_loss, position_size)
            if potential_loss >= (-1 * portfolio_size * MAX_PORTFOLIO_LOSS_PER_TRADE):
                return position_size
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
        if self.position_type == Position.POSITION_LONG and self.unrealized_pl + candle.distance_to_high >= self.take_profit:
            self.close_position(self.take_profit)
        elif self.position_type == Position.POSITION_SHORT and self.unrealized_pl + candle.distance_to_low >= self.take_profit:
            self.close_position(self.take_profit)

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

    def add_position(self, position_type, position_timestamp, atr, take_profit, stop_loss, strategy, vwap, month_vwap):
        self.positions.append(
            Position(
                position_type,
                position_timestamp,
                atr,
                take_profit,
                stop_loss,
                strategy,
                vwap,
                month_vwap
            )
        )

    def position_filter(self, position):
        # if (position.position_type == Position.POSITION_LONG and position.vwap < position.month_vwap) or \
        #     (position.position_type == Position.POSITION_SHORT and position.vwap > position.month_vwap):
        #     return False
        if NO_TRADE_AFTER_1430 and (position.timestamp.time() > (datetime.strptime("14:30:00", '%H:%M:%S').time())):
            return False
        # First position of the day
        if len([pos for pos in self.positions if pos.status is Position.POSITION_CLOSED]) > 0:
            return False
        return True

    def run_backtest(self, portfolio_size):
        for position in self.positions:
            if not self.position_filter(position):
                continue

            self.__run_backtest_for_position(position, portfolio_size)
            self.realized_pl += position.realized_pl if position.status == Position.POSITION_CLOSED else 0
            if position.status == Position.POSITION_WAITING:
                print("Position did not execute: ", position)

    def __run_backtest_for_position(self, position, portfolio_size):
        for candle in self.candles:
            if position.status == Position.POSITION_WAITING:
                if position.timestamp >= candle.timestamp and position.timestamp < candle.timestamp + timedelta(minutes=1):
                    position.open_position(candle.open, portfolio_size)

            if position.status == Position.POSITION_OPENED:
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
        self.monthly_return = {}
        
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
            session.run_backtest(self.portfolio_size)
            self.portfolio_size += session.realized_pl

            month_year = session.date.strftime('%Y-%m')
            if month_year not in self.monthly_pl:
                self.monthly_pl[month_year] = 0
            self.monthly_pl[month_year] += session.realized_pl

            if month_year not in self.monthly_return:
                self.monthly_return[month_year] = 0
                initial_portfolio_for_month = self.portfolio_size           
            self.monthly_return[month_year] = self.monthly_pl[month_year] / initial_portfolio_for_month

    def __calculate_session_pl(self):
        session_PL = {session.date.strftime('%Y-%m-%d'): session.realized_pl for session in self.sessions if len(session.positions) > 0}
        return session_PL

    def __calculate_win_ratio(self):
        no_sessions = len([session for session in self.sessions if len(session.positions) != 0])
        win_sessions = len([session for session in self.sessions if session.realized_pl >= 0 and len(session.positions) != 0])
        return win_sessions/no_sessions
    
    def __calculate_average_return(self):
        return sum(monthlyReturn for month, monthlyReturn in self.monthly_return.items()) / len(self.monthly_return)

    def __calculate_average_no_sessions_per_month(self):
        return len([session for session in self.sessions if len(session.positions) != 0]) / len(self.monthly_return)

    def print_results(self):
        print("Monthly PL: ", json.dumps(self.monthly_pl))
        print("Session PL: ", json.dumps(self.__calculate_session_pl()))
        print("Monthly return: ", self.monthly_return)
        print("Initial Portfolio: ", ENTRY_PORTFOLIO)
        print("Final Portfolio: ", self.portfolio_size)
        print("Win ratio: ", self.__calculate_win_ratio())
        print("Average monthly return: ", self.__calculate_average_return())
        print("Average no sessions per month: ", self.__calculate_average_no_sessions_per_month())


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
                    datetime.strptime(candle.date, "%Y-%m-%d").date(),
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
                    position_type,
                    datetime.strptime(f"{position.date} {position.time}", "%Y-%m-%d %H:%M:%S"),
                    position.atr,
                    position.tp if "tp" in position else "NA",
                    position.sl,
                    position.strategy,
                    position.vwap,
                    position.mvwap
                )
        else:
            print(f"Session with date {date} not found.")

    bt.run()
    bt.print_results()


if __name__ == "__main__":
    main()
