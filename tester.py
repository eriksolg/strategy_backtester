import pandas as pd
from datetime import datetime, timedelta
import json

#Constants
HISTORY_FILE = "MES_1min_continuous_adjusted.txt"
POSITION_FILE = "positions.csv"
TICK_SIZE = 0.25
BREAK_EVEN = 10
TAKE_PROFIT = 20
EXIT_PREFER = "16:00:00"
EXIT_FINAL = "16:00:00"
MAX_LOSS = -40
MAX_LOSS_PER_TRADE = -22
MIN_LOSS_PER_TRADE = -1

# Addition2: If we have not reached BREAK_EVEN after 30m, and we are in profit, BREAK_EVEN nevertheless
TIME_BASED_BREAKEVEN = False
TIME_BASED_BREAKEVEN_DURATION = timedelta(minutes=30)

# If set, stop loss amount is based on losing candle low/high, not declared stop loss. Simulate slippage.
STOP_LOSS_BASED_ON_CANDLE = False

TRAILING_STOP = 10 # Define the trailing stop distance
TRAILING_STOP_ENABLED = False # Enable or disable the trailing stop
TRIGGER_PROFIT = 15 # Define the trigger profit
CANDLE_BASED_TRAILING_STOP_ENABLED = False
CANDLE_BASED_TRAILING_STOP_LENGTH = 10

SMART_POSITION_CLOSE = False

def main():
    candleData = pd.read_csv(HISTORY_FILE)
    candleDataGroupedByDate = candleData.groupby(candleData.columns[0])

    positionData = pd.read_csv(POSITION_FILE)
    positionDataGroupedByDate = positionData.groupby(positionData.columns[0])

    for date, candles in candleDataGroupedByDate:
        candleList = []
        for index, candle in candles.iterrows():
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
    
    for date, positions in positionDataGroupedByDate:
        session = Backtest.findSession(datetime.strptime(date, "%Y-%m-%d").date())
        if session:
            for index, position in positions.iterrows():
                positionType = Position.POSITION_LONG if position.type == "L" else Position.POSITION_SHORT
                positionTrend = Position.TREND_TRENDING if "trend" in position and position.trend == "T" else Position.TREND_SIDEWAYS
                session.addPosition(
                    positionType,
                    datetime.strptime(f"{position.date} {position.time}", "%Y-%m-%d %H:%M:%S"),
                    positionTrend,
                    position.sl,
                    position.tp if "tp" in position else "NA"

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
    TREND_TRENDING = 1
    TREND_SIDEWAYS = 0

    def __init__(self, positionType, timestamp, trend, stopLossPrice, takeProfit):
        self.positionType = positionType
        self.timestamp = timestamp
        self.status = Position.POSITION_WAITING
        self.trend = trend
        self.stopLossPrice = stopLossPrice
        self.highestProfit = -999
        self.takeProfitPrice = takeProfit
        self.breakEven = BREAK_EVEN
        self.exitPrefer = EXIT_PREFER
        self.exitFinal = EXIT_FINAL
        self.maxPL = 0
        self.strategyStages = {
            "timeBasedTakeProfit": 0,
            "timeBasedBreakEven": 0
        }


    def openPosition(self, entryPrice):
        self.status = Position.POSITION_OPENED
        self.entryPrice = entryPrice
        self.unrealizedPL = 0
        self.takeProfit = abs(self.entryPrice - self.takeProfitPrice) if self.takeProfitPrice != "NA" else TAKE_PROFIT
        self.stopLoss = -1 * abs(self.stopLossPrice-entryPrice)

    def closePosition(self, pl = None):
        if pl is None:
            pl = self.unrealizedPL
        self.status = Position.POSITION_CLOSED
        self.realizedPL = pl
        self.unrealizedPL = None

    def handleUnrealizedPL(self, candle):

        if self.status is not Position.POSITION_OPENED:
            return
        if self.positionType == Position.POSITION_LONG:
            self.unrealizedPL = candle.close - self.entryPrice
        else:
            self.unrealizedPL = self.entryPrice - candle.close

        if self.unrealizedPL > self.highestProfit:
            self.highestProfit = self.unrealizedPL
            self.highestProfitLastModified = candle.timestamp


    def handleStopLoss(self, candle):
        stopLossAmount = self.stopLoss if self.stopLoss >= MAX_LOSS_PER_TRADE else MAX_LOSS_PER_TRADE
        stopLossAmount = stopLossAmount if stopLossAmount <= MIN_LOSS_PER_TRADE else MIN_LOSS_PER_TRADE
        if self.status is not Position.POSITION_OPENED:
            return
        if self.positionType == Position.POSITION_LONG and self.unrealizedPL - candle.distanceToLow <= stopLossAmount:
            self.closePosition(stopLossAmount)
        elif self.positionType == Position.POSITION_SHORT and self.unrealizedPL - candle.distanceToHigh <= stopLossAmount:
            self.closePosition(stopLossAmount)


    def handleTakeProfit(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if self.positionType == Position.POSITION_LONG and self.unrealizedPL + candle.distanceToHigh >= self.takeProfit:
            self.closePosition(self.takeProfit)
        elif self.positionType == Position.POSITION_SHORT and self.unrealizedPL + candle.distanceToLow >= self.takeProfit:
            self.closePosition(self.takeProfit)

    def handleTrailingStop(self, candle):
        global TRIGGER_PROFIT
        global TRAILING_STOP
        if self.status is not Position.POSITION_OPENED or not TRAILING_STOP_ENABLED:
            return

        if self.positionType == Position.POSITION_LONG and self.unrealizedPL + candle.distanceToHigh >= TRIGGER_PROFIT:
            self.stopLoss = TRIGGER_PROFIT - TRAILING_STOP
            TRIGGER_PROFIT = TRIGGER_PROFIT + 10

        elif self.positionType == Position.POSITION_SHORT and self.unrealizedPL + candle.distanceToLow >= TRIGGER_PROFIT:
            self.stopLoss = TRIGGER_PROFIT - TRAILING_STOP
            TRIGGER_PROFIT = TRIGGER_PROFIT + 10


    def handleCandleBasedTrailingStop(self, candleGroup):
        if self.status is not Position.POSITION_OPENED or not CANDLE_BASED_TRAILING_STOP_ENABLED:
            return

        
        if self.positionType == Position.POSITION_LONG:
            if candleGroup[0].lowPrice - self.entryPrice < self.unrealizedPL:
                self.stopLoss = candleGroup[0].lowPrice - self.entryPrice
        if self.positionType == Position.POSITION_SHORT:
            if candleGroup[0].highPrice - self.entryPrice < self.unrealizedPL:
                self.stopLoss = candleGroup[0].highPrice - self.entryPrice

    def handleSmartPositionClose(self, candle):
        global SMART_POSITION_CLOSE
        if self.status is not Position.POSITION_OPENED or not SMART_POSITION_CLOSE:
            return

        if candle.timestamp - self.highestProfitLastModified > timedelta(minutes = 60) and self.unrealizedPL > 0 and self.highestProfit - self.unrealizedPL < 5:
            print("closing smart")
            self.closePosition()

    def handleBreakEven(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if ((self.positionType == Position.POSITION_LONG and self.unrealizedPL + candle.distanceToHigh >= self.breakEven) or
            (self.positionType == Position.POSITION_SHORT and self.unrealizedPL + candle.distanceToLow >= self.breakEven)) and self.stopLoss<0:
            self.stopLoss = 0

        if self.status is Position.POSITION_OPENED and TIME_BASED_BREAKEVEN and self.stopLoss<0:
            if candle.timestamp - self.timestamp >= TIME_BASED_BREAKEVEN_DURATION and self.unrealizedPL >= 0:
                self.stopLoss = 0


    def handleEndOfDay(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if candle.timestamp.time() >= datetime.strptime(self.exitPrefer, '%H:%M:%S').time():
            if self.unrealizedPL >= 0 or (candle.timestamp.time() >= (datetime.strptime(self.exitFinal, '%H:%M:%S').time())):
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

        self.openLow = candles[0].lowPrice
        self.openHigh = candles[2].highPrice

    def addPosition(self, positionType, positionTimestamp, trend, positionStopLoss, takeProfit):
        self.positions.append(
            Position(
                positionType,
                positionTimestamp,
                trend,
                positionStopLoss,
                takeProfit
            )
        )

    def runBackTest(self):
        for position in self.positions:
            self.__runBackTestForPosition(position)
            self.realizedPL += position.realizedPL if position.status == Position.POSITION_CLOSED else 0
            if position.status == Position.POSITION_WAITING:
                print("Position did not execute: ", position)
    
    def __calculateEntryPrice(self, position, candle):
        entryPercentage = position.timestamp.second / 60
        if candle.direction == Candle.DIRECTION_BULL:
            entryPriceCalculated = candle.lowPrice + candle.delta * entryPercentage
        elif candle.direction == Candle.DIRECTION_BEAR:
            entryPriceCalculated = candle.highPrice - candle.delta * entryPercentage
        else:
            entryPriceCalculated = candle.open
    
        return round(entryPriceCalculated / TICK_SIZE) * TICK_SIZE


    def __runBackTestForPosition(self, position):
        
        candleGroup = []
        candleGroupIterations = 0
        for candle in self.candles:
            if position.status == Position.POSITION_WAITING:
                if position.timestamp >= candle.timestamp and position.timestamp < candle.timestamp + timedelta(minutes=1):
                    entryPrice = self.__calculateEntryPrice(position, candle)
                    position.openPosition(entryPrice)                

            if position.status == Position.POSITION_OPENED:
                candleGroup.append(candle)

                position.handleStopLoss(candle)
                position.handleTakeProfit(candle)
                position.handleBreakEven(candle)
                position.handleEndOfDay(candle)
                position.handleUnrealizedPL(candle)
                position.handleTrailingStop(candle)
                position.handleSmartPositionClose(candle)

                if len(candleGroup) == CANDLE_BASED_TRAILING_STOP_LENGTH:
                    candleGroupIterations += 1
                    position.handleCandleBasedTrailingStop(candleGroup)
                    previousCandleGroup = candleGroup
                    candleGroup = []

                # if len(candleGroup) == CANDLE_BASED_TRAILING_STOP_LENGTH/2 and candleGroupIterations != 0:
                #     candleGroupIterations += 1
                #     position.handleCandleBasedTrailingStop(candleGroup)
                #     candleGroup = candleGroup[int(CANDLE_BASED_TRAILING_STOP_LENGTH/2):]

                # if position.positionType == Position.POSITION_LONG and len(candleGroup) == CANDLE_BASED_TRAILING_STOP_LENGTH/2 and candle.close < candleGroup[0].open and len(previousCandleGroup) > 0:
                #     candleGroupIterations += 1
                #     position.handleCandleBasedTrailingStop(previousCandleGroup[int(CANDLE_BASED_TRAILING_STOP_LENGTH/2):])
                #     candleGroup = []
                # if position.positionType == Position.POSITION_SHORT and len(candleGroup) == CANDLE_BASED_TRAILING_STOP_LENGTH/2 and candle.close > candleGroup[0].open and len(previousCandleGroup) > 0:
                #     candleGroupIterations += 1
                #     position.handleCandleBasedTrailingStop(previousCandleGroup[int(CANDLE_BASED_TRAILING_STOP_LENGTH/2):])
                #     candleGroup = []
                
                # if candleGroupIterations >= 2 and len(candleGroup) == CANDLE_BASED_TRAILING_STOP_LENGTH/2:
                #     candleGroupIterations += 1
                #     position.handleCandleBasedTrailingStop(candleGroup)
                #     candleGroup = []

    def __str__(self):
        return str(f"Session: {self.date}")

    def __repr__(self):
        return self.__str__()


class Backtest:
    sessions = []
    results = []

    def addSession(session):
        Backtest.sessions.append(session)

    def findSession(date):
        for session in Backtest.sessions:
            if session.date == date:
                return session
        return None

    def run():
        
        for session in Backtest.sessions:
            session.runBackTest()

    def calculateTotalPL():
        total_PL = sum(session.realizedPL for session in Backtest.sessions if session.exceedMonthlyPL == False)
        return total_PL

    def calculateMonthlyPL():
        monthly_PL = {}
        for session in Backtest.sessions:
            month_year = session.date.strftime('%Y-%m') # Format the date as 'YYYY-MM'
            if month_year not in monthly_PL:
                monthly_PL[month_year] = 0
            if monthly_PL[month_year] < MAX_LOSS:
                session.exceedMonthlyPL == True
                continue
            monthly_PL[month_year] += session.realizedPL
        return monthly_PL

    def calculateSessionPL():
        session_PL = {session.date.strftime('%Y-%m-%d'): session.realizedPL for session in Backtest.sessions if len(session.positions) > 0}
        return session_PL

    def calculateWinRatio():
        no_sessions = len(Backtest.sessions)
        win_sessions = len([session for session in Backtest.sessions if session.realizedPL > 0])
        return win_sessions/no_sessions

    def printResults():
        global TAKE_PROFIT
        global BREAK_EVEN
        global EXIT_PREFER

        print(f"Take Profit: {TAKE_PROFIT}")
        print(f"Break Even: {BREAK_EVEN}")
        print(f"Exit: {EXIT_PREFER}")
        print("")
        print("Monthly PL: ", json.dumps(Backtest.calculateMonthlyPL()))
        print("Session PL: ", json.dumps(Backtest.calculateSessionPL()))
        print("Total PL: ", Backtest.calculateTotalPL())
        print("Win ratio: ", Backtest.calculateWinRatio())

    def writeResultsToCSV(self, filename):
        results = pd.DataFrame({
            'Month': list(Backtest.calculateMonthlyPL().keys()),
            'PL': list(Backtest.calculateMonthlyPL().values())
        })
        results.to_csv(filename, index=False)

if __name__ == "__main__":
    main()