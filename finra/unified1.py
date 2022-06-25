import time
from util import *
import json
import datetime

import pickle

################## FETCH MARKET DATA
def fetchMarketData(event):
    ### disaggr get begin
    ### disaggr get end

    ### compute begin
    portfolioType = event['body']['portfolioType']
    tickersForPortfolioTypes = {'S&P': ['GOOG', 'AMZN', 'MSFT']}
    tickers = tickersForPortfolioTypes[portfolioType]

    #prices = {}
    #for ticker in tickers:
        #tickerObj = yf.Ticker(ticker)
        #Get last closing price
        #tickTime = 1000*time.time()
        #data = tickerObj.history(period="1")
        #externalServicesTime += 1000*time.time() - tickTime
        #price = data['Close'].unique()[0]
        #prices[ticker] = price

    prices = {'GOOG': 1732.38, 'AMZN': 3185.27, 'MSFT': 221.02}
    response = {'body': {'marketData':prices}}
    ### compute end

    ## disaggr put begin
    event_response = pickle.dumps(response)
    ### disaggr put end
    
    return event_response
    

################## FETCH PORTFOLIO DATA
def fetchPortfoliosData(event):
    ### disaggr get begin
    ### disaggr get end

    ### compute begin
    portfolios = json.loads(open('data/portfolios.json', 'r').read())
    event_response = pickle.dumps(portfolios)
    ### compute begin
    
    return event_response
    
################## TRDATE
def trdate(event, portfolios_pickle):
    ### disaggr get begin
    portfolio = event['body']['portfolio']
    portfolios = pickle.loads(portfolios_pickle)
    data = portfolios[portfolio]
    ### disaggr get end
    
    ### compute begin
    valid = True

    for trade in data:
        trddate = trade['TradeDate']
        # Tag ID: 75, Tag Name: TradeDate, Format: YYMMDD
        if len(trddate) == 6:
            try:
                datetime.datetime(int(trddate[0:2]), int(trddate[2:4]), int(trddate[4:6]))
            except ValueError:
                valid = False
                break
        else:
            valid = False
            break

    response = {'body': {'valid':valid, 'portfolio': portfolio}}
    ### compute end    

    return response

################## VOLUME
def volume(event, portfolios_pickle):
    ### disaggr get begin
    portfolio = event['body']['portfolio']
    portfolios = pickle.loads(portfolios_pickle)
    data = portfolios[portfolio]
    ### disaggr get end

    ### compute begin
    valid = True

    for trade in data:
        qty = str(trade['LastQty'])
        # Tag ID: 32, Tag Name: LastQty, Format: max 8 characters, no decimal
        if (len(qty)>8) or ('.'in qty):
            valid = False
            break
    response = {'body': {'valid':valid, 'portfolio': portfolio}}
    ### compute end
    
    return response

################## SIDE
def side(event, portfolios_pickle):
    ### disaggr get begin
    portfolio = event['body']['portfolio']
    portfolios = pickle.loads(portfolios_pickle)
    data = portfolios[portfolio]
    ### disaggr get end
    
    ### compute begin    
    valid = True

    for trade in data:
        side = trade['Side']
        # Tag ID: 552, Tag Name: Side, Valid values: 1,2,8
        if not (side == 1 or side == 2 or side == 8):
            valid = False
            break

    response = {'body': {'valid':valid, 'portfolio': portfolio}}
    ### compute end    
    
    return response

################## LASTPX
def lastpx(event, portfolios_pickle):
    ### disaggr get begin
    portfolio = event['body']['portfolio']
    portfolios = pickle.loads(portfolios_pickle)
    data = portfolios[portfolio]
    ### disaggr get end

    ### compute begin
    valid = True

    for trade in data:
        px = str(trade['LastPx'])
        if '.' in px:
            a,b = px.split('.')
            if not ((len(a) == 3 and len(b) == 6) or
                    (len(a) == 4 and len(b) == 5) or
                    (len(a) == 5 and len(b) == 4) or
                    (len(a) == 6 and len(b) == 3)):
                print('{}: {}v{}'.format(px, len(a), len(b)))
                valid = False
                break

    response = {'body': {'valid':valid, 'portfolio': portfolio}}
    ### compute end
    
    return response

################## CHECK MARGIN BALANCE
def checkMarginBalance(marginAccountBalance, portfolioData, marketData, portfolio):
    #marginAccountBalance = json.loads(open('data/marginBalance.json', 'r').read())[portfolio]

    portfolioMarketValue = 0
    for trade in portfolioData:
        security = trade['Security']
        qty = trade['LastQty']
        portfolioMarketValue += qty*marketData[security]

    #Maintenance Margin should be atleast 25% of market value for "long" securities
    #https://www.finra.org/rules-guidance/rulebooks/finra-rules/4210#the-rule
    result = False
    if marginAccountBalance >= 0.25*portfolioMarketValue:
        result = True

    return result


def marginBalance(valid_events, portfolios_pickle, marketdata_pickle, marginbalance_pickle):
    ### disaggr get begin
    portfolios = pickle.loads(portfolios_pickle)
    marketdata = pickle.loads(marketdata_pickle)
    margindata = pickle.loads(marginbalance_pickle)
    ### disaggr get end

    ### compute begin
    validFormat = True
    for event in valid_events:
        validFormat = validFormat and event['body']['valid']
    # check margin balance only if the portfolio is valid
    if validFormat==True:
        marginSatisfied = checkMarginBalance(margindata['1234'], portfolios['1234'], marketdata['body']['marketData'], '1234')
    ### compute end  
    
    ### put begin
    portfolios['1234'].append({'validFormat':validFormat, 'marginSatisfied':marginSatisfied})
    pickle.dumps(portfolios)
    ### put end

################## MAIN
# we assume the entire pipeline is invoked per portfolio
# the object store contains all portfolios and all marketdata and all margin balances
# marginBalance checks for validity of all rules on the portfolio and the margin balance
# marginBalance updates the portfolio with valid true/false and marginSatisfied true/false
print("********FETCH MARKET DATA")
event = {'body':{'portfolioType':'S&P'}}
marketdata_pickle = fetchMarketData(event)

print("********FETCH PORTFOLIOS DATA")
portfolios_pickle = fetchPortfoliosData(event)

print("********RUN AUDIT RULES IN PARALLEL")

portfolio_event = {"body": {"portfolioType": "S&P","portfolio": "1234"}}

valid = []
print("********VOLUME")
valid.append(volume(portfolio_event, portfolios_pickle))

print("********TRDATE")
valid.append(trdate(portfolio_event, portfolios_pickle))

print("********SIDE")
valid.append(side(portfolio_event, portfolios_pickle))

print("********LASTPX")
valid.append(lastpx(portfolio_event, portfolios_pickle))

print("********checkMarginBalance")
marginbalance_pickle = pickle.dumps(json.loads(open('data/marginBalance.json', 'r').read()))
marginBalance(valid, portfolios_pickle, marketdata_pickle, marginbalance_pickle)

