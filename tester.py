import pandas as pd
from datetime import datetime, date, timedelta

#Constants
HISTORY_FILE = "MES_1min_continuous_adjusted.txt"
POSITION_FILE = "positions.csv"
TICK_SIZE = 0.25
STOP_LOSS = -12
BREAK_EVEN = 5
TAKE_PROFIT = 15
EXIT_PREFER = "16:00:00"
EXIT_FINAL = "16:00:00"
MAX_LOSS = 40
MAX_LOSS_PER_TRADE = -12

# Addition1: If we do not get 1/3 of Take profit within first hour after opening the position,
# decrease Take profit to 2/3.
# If we do not get 2/3 Take profit within second hour after opening the position,
# decrease Take profit to 1/3.
# If we do not get 1/3 take profit after third hour, and we are in profit, take profit nevertheless.
TIME_BASED_TAKE_PROFIT = False

# Addition2: If we have not reached BREAK_EVEN after 30m, and we are in profit, BREAK_EVEN nevertheless
TIME_BASED_BREAKEVEN = False
TIME_BASED_BREAKEVEN_DURATION = timedelta(minutes=30)

# If set, stop loss amount is based on losing candle low/high, not declared stop loss.
STOP_LOSS_BASED_ON_CANDLE = False


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
                    position.sl

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

    def __init__(self, positionType, timestamp, trend, stopLossPrice):
        self.positionType = positionType
        self.timestamp = timestamp
        self.status = Position.POSITION_WAITING
        self.trend = trend
        self.stopLossPrice = stopLossPrice
        self.takeProfit = TAKE_PROFIT
        self.breakEven = BREAK_EVEN
        self.exitPrefer = EXIT_PREFER
        self.exitFinal = EXIT_FINAL
        self.strategyStages = {
            "timeBasedTakeProfit": 0,
            "timeBasedBreakEven": 0
        }


    def openPosition(self, entryPrice):
        self.status = Position.POSITION_OPENED
        self.entryPrice = entryPrice
        self.unrealizedPL = 0
        self.stopLoss = -1 * abs(self.stopLossPrice-entryPrice)

    def closePosition(self, pl = None):
        if pl is None:
            pl = self.unrealizedPL
        self.status = Position.POSITION_CLOSED
        self.realizedPL = pl
        self.unrealizedPL = None
        print(self.timestamp)
        print(self.realizedPL)

    def handleUnrealizedPL(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if self.positionType == Position.POSITION_LONG:
            self.unrealizedPL = candle.close - self.entryPrice
        else:
            self.unrealizedPL = self.entryPrice - candle.close

    def handleStopLoss(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if self.positionType == Position.POSITION_LONG and self.unrealizedPL - candle.distanceToLow <= self.stopLoss:
            stopLossAmount = self.unrealizedPL - candle.distanceToLow if STOP_LOSS_BASED_ON_CANDLE else self.stopLoss
            stopLossAmount = stopLossAmount if stopLossAmount >= MAX_LOSS_PER_TRADE else MAX_LOSS_PER_TRADE
            self.closePosition(stopLossAmount)
        elif self.positionType == Position.POSITION_SHORT and self.unrealizedPL - candle.distanceToHigh <= self.stopLoss:
            stopLossAmount = self.unrealizedPL - candle.distanceToHigh if STOP_LOSS_BASED_ON_CANDLE else self.stopLoss
            stopLossAmount = stopLossAmount if stopLossAmount >= MAX_LOSS_PER_TRADE else MAX_LOSS_PER_TRADE
            self.closePosition(stopLossAmount)


    def handleTakeProfit(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if self.positionType == Position.POSITION_LONG and self.unrealizedPL + candle.distanceToHigh >= self.takeProfit:
            self.closePosition(self.takeProfit)
        elif self.positionType == Position.POSITION_SHORT and self.unrealizedPL + candle.distanceToLow >= self.takeProfit:
            self.closePosition(self.takeProfit)


# Addition1:
# 1h passed. Profit < 1/3 TP? TP to 1/3
# 1h passed. Profit < 2/3 TP? TP to 2/3
# 2h passed. Have not reached TP of 1/3 and in +? Close position.
# 2h passed. Have not reached TP of 2/3 and Profit < 1/3?  TP to 1/3
# 2h passed. Have not reached TP of 2/3,but Profit is > 1/3? SL to 1/3
# decrease Take profit to 2/3.
# If we do not get 2/3 Take profit within second hour after opening the position,
# decrease Take profit to 1/3.
# If we do not get 1/3 take profit after third hour, and we are in profit, take profit nevertheless.
        if self.status is Position.POSITION_OPENED and TIME_BASED_TAKE_PROFIT:
            # Takes too much time
            if candle.timestamp - self.timestamp >= timedelta(hours=1) and self.strategyStages["timeBasedTakeProfit"] == 0:
                self.strategyStages["timeBasedTakeProfit"] = 1
                if self.positionType == Position.POSITION_LONG and self.unrealizedPL + candle.distanceToHigh < (self.takeProfit/3):
                    self.takeProfit = self.takeProfit/3
                elif self.positionType == Position.POSITION_SHORT and self.unrealizedPL + candle.distanceToLow < (self.takeProfit/3):
                    self.takeProfit = self.takeProfit/3     
                elif self.positionType == Position.POSITION_LONG and self.unrealizedPL + candle.distanceToHigh < (self.takeProfit/3*2):
                    self.takeProfit = self.takeProfit/3*2
                elif self.positionType == Position.POSITION_SHORT and self.unrealizedPL + candle.distanceToLow < (self.takeProfit/3):
                    self.takeProfit = self.takeProfit/3*2

            elif candle.timestamp - self.timestamp >= timedelta(hours=2) and self.strategyStages["timeBasedTakeProfit"] == 1:
                self.strategyStages["timeBasedTakeProfit"] = 2
                if self.takeProfit == TAKE_PROFIT/3 and self.unrealizedPL > 0:
                    self.closePosition()
                elif self.takeProfit == TAKE_PROFIT/3*2:
                    if self.positionType == Position.POSITION_LONG and self.unrealizedPL + candle.distanceToHigh < (self.takeProfit/2):
                        self.takeProfit = self.takeProfit/2
                    elif self.positionType == Position.POSITION_SHORT and self.unrealizedPL + candle.distanceToLow < (self.takeProfit/2):
                        self.takeProfit = self.takeProfit/2
                    else:
                        self.stopLoss = self.takeProfit/2


    def handleBreakEven(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if ((self.positionType == Position.POSITION_LONG and self.unrealizedPL + candle.distanceToHigh >= self.breakEven) or
            (self.positionType == Position.POSITION_SHORT and self.unrealizedPL + candle.distanceToLow >= self.breakEven)):
            self.stopLoss = 0

        if self.status is Position.POSITION_OPENED and TIME_BASED_BREAKEVEN:
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
        self.maxLoss = MAX_LOSS

        self.openLow = candles[0].lowPrice
        self.openHigh = candles[2].highPrice

    def addPosition(self, positionType, positionTimestamp, trend, positionStopLoss):
        self.positions.append(
            Position(
                positionType,
                positionTimestamp,
                trend,
                positionStopLoss
            )
        )

    def runBackTest(self):
        for position in self.positions:
            self.__runBackTestForPosition(position)
            self.realizedPL += position.realizedPL
            if self.realizedPL <= self.maxLoss:
                break
    
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
        
        for candle in self.candles:
            if position.status == Position.POSITION_WAITING:
                if position.timestamp >= candle.timestamp and position.timestamp < candle.timestamp + timedelta(minutes=1):
                    entryPrice = self.__calculateEntryPrice(position, candle)
                    position.openPosition(entryPrice)

            elif position.status == Position.POSITION_OPENED:

                position.handleStopLoss(candle)
                position.handleTakeProfit(candle)
                position.handleBreakEven(candle)
                position.handleEndOfDay(candle)
                position.handleUnrealizedPL(candle)

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
        total_PL = sum(session.realizedPL for session in Backtest.sessions)
        return total_PL

    def calculateMonthlyPL():
        monthly_PL = {}
        for session in Backtest.sessions:
            month_year = session.date.strftime('%Y-%m') # Format the date as 'YYYY-MM'
            if month_year not in monthly_PL:
                monthly_PL[month_year] = 0
            monthly_PL[month_year] += session.realizedPL
        return monthly_PL

    def calculateSessionPL():
        session_PL = {session.date: session.realizedPL for session in Backtest.sessions if len(session.positions) > 0}
        return session_PL

    def printResults():
        global STOP_LOSS
        global TAKE_PROFIT
        global BREAK_EVEN
        global EXIT_PREFER

        print(f"Stop Loss: {STOP_LOSS}")
        print(f"Take Profit: {TAKE_PROFIT}")
        print(f"Break Even: {BREAK_EVEN}")
        print(f"Exit: {EXIT_PREFER}")
        print("")
        print("Total PL: ", Backtest.calculateTotalPL())
        print("Monthly PL: ", Backtest.calculateMonthlyPL())
        print("Session PL: ", Backtest.calculateSessionPL())

    def writeResultsToCSV(self, filename):
        results = pd.DataFrame({
            'Month': list(Backtest.calculateMonthlyPL().keys()),
            'PL': list(Backtest.calculateMonthlyPL().values())
        })
        results.to_csv(filename, index=False)

if __name__ == "__main__":
    main()