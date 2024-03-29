import pandas as pd
from datetime import datetime, timedelta
import json
import math
import os
import pickle

#Constants
HISTORY_FILE = "MES_1min_continuous_adjusted.txt"
POSITION_FILE = "positions.csv"
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
    "rsi": 2,
    "ret": 2,
    "retw": 2,
    "brk": 2
}

def main():
    candleData = pd.read_csv(HISTORY_FILE)

    candleDataGroupedByDate = candleData.groupby(candleData.columns[0])

    positionData = pd.read_csv(POSITION_FILE)
    positionDataGroupedByDate = positionData.groupby(positionData.columns[0])

    Backtest.load_sessions_from_cache()
    if len(Backtest.sessions) == 0:

        for date, candles in candleDataGroupedByDate:
            candleList = []
            for _, candle in candles.iterrows():
                candleList.append(
                    Candle(
                        datetime.strptime(f"{candle.date} {candle.time}", "%Y-%m-%d %H:%M:%S"),
                        candle.open,
                        candle.close,
                        candle.low,
                        candle.high,
                        candle.volume
                    )
                )

            Backtest.addSession(
                Session(
                    datetime.strptime(candle.date, "%Y-%m-%d").date(),
                    candleList
                )
            )
        Backtest.save_sessions_to_cache()

    
    for date, positions in positionDataGroupedByDate:
        session = Backtest.findSession(datetime.strptime(date, "%Y-%m-%d").date())
        if session:
            for _, position in positions.iterrows():
                positionType = Position.POSITION_LONG if position.type == "L" else Position.POSITION_SHORT
                session.addPosition(
                    positionType,
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


    Backtest.run()
    Backtest.printResults()

class Candle:
    DIRECTION_BULL = 1
    DIRECTION_BEAR = -1
    DIRECTION_NEUTRAL = 0

    def __init__(self, timestamp, openPrice, closePrice, lowPrice, highPrice, volume):
        self.timestamp = timestamp
        self.open = openPrice
        self.close = closePrice
        self.lowPrice = lowPrice
        self.highPrice = highPrice
        self.volume = volume
        self.direction = self.__calculate_direction()
        self.delta = self.__calculate_delta()
        self.distanceToHigh = abs(self.open - self.highPrice)
        self.distanceToLow = abs(self.open - self.lowPrice)


    def __calculate_direction(self):
       if self.close > self.open:
           return self.DIRECTION_BULL
       elif self.close < self.open:
           return self.DIRECTION_BEAR
       else:
           return self.DIRECTION_NEUTRAL

    def __calculate_delta(self):
        return abs(self.lowPrice - self.highPrice)

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

    def __init__(self, positionType, timestamp, atr, takeProfit, stopLoss, strategy, vwap, monthVwap):
        self.positionType = positionType
        self.timestamp = timestamp
        self.status = Position.POSITION_WAITING
        self.takeProfitPrice = takeProfit
        self.exitFinal = EXIT_FINAL
        self.atr = atr
        self.stopLossPrice = stopLoss
        self.strategy = strategy
        self.breakEven = BREAK_EVEN_ATR[self.strategy] * self.atr
        self.vwap = vwap
        self.monthVwap = monthVwap

    def openPosition(self, entryPrice):
        self.status = Position.POSITION_OPENED
        self.entryPrice = entryPrice
        self.unrealizedPL = 0
        self.takeProfit = abs(self.entryPrice - self.takeProfitPrice)
        if RSI_TP_3ATR and self.strategy == "rsi":
            self.takeProfit = 3 * self.atr
        if math.isnan(self.stopLossPrice):
            self.stopLoss = -2 * self.atr
        else:
            self.stopLoss = -1 * abs(self.stopLossPrice-entryPrice)
        positionSize = self.calculateInitialPositionSize()
        if positionSize != None:
            self.positionSize = positionSize
        else:
            print(f"Cannot open position for ${self}. Not enough margin to cover stop loss.")
            self.positionSize = 0
            self.closePosition(status = Position.POSITION_DISCARDED)

    def calculateInitialPositionSize(self):
        marginRequirement = self.entryPrice * MAINTENANCE_MARGIN
        maxPositions = int(Backtest.portFolioSize // marginRequirement)

        for positionSize in range(maxPositions, 1, -1):
            potentialLoss = self.getValueInUSD(self.stopLoss, positionSize)
            if potentialLoss >= (-1 * Backtest.portFolioSize * MAX_PORTFOLIO_LOSS_PER_TRADE):
                return positionSize
        return None

    def closePosition(self, pl = None, status = POSITION_CLOSED):
        if pl is None:
            pl = self.unrealizedPL
        self.status = status
        self.realizedPL = self.getValueInUSD(pl, self.positionSize)
        self.unrealizedPL = None

    def handleUnrealizedPL(self, candle):
        if self.positionType == Position.POSITION_LONG:
            self.unrealizedPL = candle.open - self.entryPrice
        else:
            self.unrealizedPL = self.entryPrice - candle.open

    def getValueInUSD(self, valueInPoints, positionSize):
        return (valueInPoints / TICK_SIZE) * VALUE_OF_TICK * positionSize

    def handleStopLoss(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if (self.positionType == Position.POSITION_LONG and self.unrealizedPL - candle.distanceToLow <= self.stopLoss) or \
            (self.positionType == Position.POSITION_SHORT and self.unrealizedPL - candle.distanceToHigh <= self.stopLoss):
            self.closePosition(self.stopLoss)


    def handleTakeProfit(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if self.positionType == Position.POSITION_LONG and self.unrealizedPL + candle.distanceToHigh >= self.takeProfit:
            self.closePosition(self.takeProfit)
        elif self.positionType == Position.POSITION_SHORT and self.unrealizedPL + candle.distanceToLow >= self.takeProfit:
            self.closePosition(self.takeProfit)

    def handleBreakEven(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if ((self.positionType == Position.POSITION_LONG and self.unrealizedPL + candle.distanceToHigh >= self.breakEven) or
            (self.positionType == Position.POSITION_SHORT and self.unrealizedPL + candle.distanceToLow >= self.breakEven)) and self.stopLoss<0:
            self.stopLoss = 0

    def handleEndOfDay(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if (candle.timestamp.time() >= (datetime.strptime(self.exitFinal, '%H:%M:%S').time())):
            self.closePosition()

    def __str__(self):
        return str(f"Position({self.timestamp})")

    def __repr__(self):
        return self.__str__()

class Session:
    def __init__(self, date, candles):
        self.date = date
        self.candles = candles
        self.positions = []
        self.realizedPL = 0
        self.exceedMonthlyPL = False

    def addPosition(self, positionType, positionTimestamp, atr, takeProfit, stopLoss, strategy, vwap, monthVwap):
        newPosition = Position(
                positionType,
                positionTimestamp,
                atr,
                takeProfit,
                stopLoss,
                strategy,
                vwap,
                monthVwap
        )

        self.positions.append(newPosition)

    def positionFilter(self, position):
        # if (position.positionType == Position.POSITION_LONG and position.vwap < position.monthVwap) or \
        #     (position.positionType == Position.POSITION_SHORT and position.vwap > position.monthVwap):
        #     return False
        if NO_TRADE_AFTER_1430 and (position.timestamp.time() > (datetime.strptime("14:30:00", '%H:%M:%S').time())):
            return False
        # First position of the day
        if len([pos for pos in self.positions if pos.status is Position.POSITION_CLOSED]) > 0:
            return False
        return True

    def runBackTest(self):
        for position in self.positions:
            if not self.positionFilter(position):
                continue

            self.__runBackTestForPosition(position)
            self.realizedPL += position.realizedPL if position.status == Position.POSITION_CLOSED else 0
            if position.status == Position.POSITION_WAITING:
                print("Position did not execute: ", position)

    def __runBackTestForPosition(self, position):
        for candle in self.candles:
            if position.status == Position.POSITION_WAITING:
                if position.timestamp >= candle.timestamp and position.timestamp < candle.timestamp + timedelta(minutes=1):
                    position.openPosition(candle.open)

            if position.status == Position.POSITION_OPENED:
                position.handleUnrealizedPL(candle)
                position.handleStopLoss(candle)
                position.handleTakeProfit(candle)
                position.handleBreakEven(candle)
                position.handleEndOfDay(candle)

    def __str__(self):
        return str(f"Session: {self.date}")

    def __repr__(self):
        return self.__str__()


class Backtest:
    portFolioSize = ENTRY_PORTFOLIO
    sessions = []
    results = []
    monthly_PL = {}
    monthlyReturn = {}
    CACHE_FILE = 'backtest_sessions_cache.pkl'

    @classmethod
    def load_sessions_from_cache(cls):
        if os.path.exists(cls.CACHE_FILE):
            with open(cls.CACHE_FILE, 'rb') as f:
                cls.sessions = pickle.load(f)

    @classmethod
    def save_sessions_to_cache(cls):
        with open(cls.CACHE_FILE, 'wb') as f:
            pickle.dump(cls.sessions, f)

    def addSession(session):
        Backtest.sessions.append(session)

    def findSession(date):
        for session in Backtest.sessions:
            if session.date == date:
                return session
        return None

    def run():
        for session in Backtest.sessions:
            if len(session.positions) == 0:
                continue
            session.runBackTest()
            Backtest.portFolioSize += session.realizedPL

            month_year = session.date.strftime('%Y-%m')
            if month_year not in Backtest.monthly_PL:
                Backtest.monthly_PL[month_year] = 0
            Backtest.monthly_PL[month_year] += session.realizedPL

            if month_year not in Backtest.monthlyReturn:
                Backtest.monthlyReturn[month_year] = 0
                initialPortfolioForMonth = Backtest.portFolioSize           
            Backtest.monthlyReturn[month_year] = Backtest.monthly_PL[month_year] / initialPortfolioForMonth

    def calculateTotalPL():
        return sum(session.realizedPL for session in Backtest.sessions)

    def calculateSessionPL():
        session_PL = {session.date.strftime('%Y-%m-%d'): session.realizedPL for session in Backtest.sessions if len(session.positions) > 0}
        return session_PL

    def calculateWinRatio():
        no_sessions = len([session for session in Backtest.sessions if len(session.positions) != 0])
        win_sessions = len([session for session in Backtest.sessions if session.realizedPL >= 0 and len(session.positions) != 0])
        return win_sessions/no_sessions
    
    def calculateAverageReturn():
        return sum(monthlyReturn for month, monthlyReturn in Backtest.monthlyReturn.items()) / len(Backtest.monthlyReturn)

    def calculateAverageNoSessionsPerMonth():
        return len([session for session in Backtest.sessions if len(session.positions) != 0]) / len(Backtest.monthlyReturn)

    def printResults():
        print("Monthly PL: ", json.dumps(Backtest.monthly_PL))
        print("Session PL: ", json.dumps(Backtest.calculateSessionPL()))
        print("Monthly return: ", Backtest.monthlyReturn)
        print("Initial Portfolio: ", ENTRY_PORTFOLIO)
        print("Final Portfolio: ", Backtest.portFolioSize)
        print("Win ratio: ", Backtest.calculateWinRatio())
        print("Average monthly return: ", Backtest.calculateAverageReturn())
        print("Average no sessions per month: ", Backtest.calculateAverageNoSessionsPerMonth())

    def writeResultsToCSV(self, filename):
        results = pd.DataFrame({
            'Month': list(Backtest.calculateMonthlyPL().keys()),
            'PL': list(Backtest.calculateMonthlyPL().values())
        })
        results.to_csv(filename, index=False)

if __name__ == "__main__":
    main()
