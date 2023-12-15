import pandas as pd
from datetime import datetime, date, timedelta

#Constants
HISTORY_FILE = "MES_1min_continuous_adjusted.txt"
POSITION_FILE = "testdata.csv"
TICK_SIZE = 0.25
STOP_LOSS = -2
BREAK_EVEN = 10
TAKE_PROFIT = 20
EXIT_PREFER = "15:00:00"
EXIT_FINAL = "16:00:00"

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
                session.addPosition(
                    positionType,
                    datetime.strptime(f"{position.date} {position.time}", "%Y-%m-%d %H:%M:%S")
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

    def __init__(self, positionType, timestamp):
        self.positionType = positionType
        self.timestamp = timestamp
        self.status = Position.POSITION_WAITING
        self.stopLoss = STOP_LOSS
        self.takeProfit = TAKE_PROFIT
        self.breakEven = BREAK_EVEN
        self.exitPrefer = EXIT_PREFER
        self.exitFinal = EXIT_FINAL

    def openPosition(self, entryPrice):
        self.status = Position.POSITION_OPENED
        self.entryPrice = entryPrice
        self.unrealizedPL = 0

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

    def handleStopLoss(self, candle):
        if self.status is not Position.POSITION_OPENED:
            return
        if self.positionType == Position.POSITION_LONG and self.unrealizedPL - candle.distanceToLow <= self.stopLoss:
            self.closePosition(self.unrealizedPL - candle.distanceToLow)
        elif self.positionType == Position.POSITION_SHORT and self.unrealizedPL - candle.distanceToHigh <= self.stopLoss:
            self.closePosition(self.unrealizedPL - candle.distanceToHigh)


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
            (self.positionType == Position.POSITION_SHORT and self.unrealizedPL + candle.distanceToLow >= self.breakEven)):
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

        self.openLow = candles[0].lowPrice
        self.openHigh = candles[2].highPrice

    def addPosition(self, positionType, positionTimestamp):
        self.positions.append(
            Position(
                positionType,
                positionTimestamp
            )
        )

    def runBackTest(self):
        for position in self.positions:
            self.__runBackTestForPosition(position)
            self.realizedPL += position.realizedPL
    
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