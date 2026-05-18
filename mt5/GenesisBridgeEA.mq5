//+------------------------------------------------------------------+
//| GenesisBridgeEA.mq5                                             |
//| Genesis MT5 Bridge - demo/backtest/journal first.                |
//| No real trading by default.                                      |
//+------------------------------------------------------------------+
#property strict
#property version   "11.11"
#property description "Genesis MT5 Bridge EA. Journal/demo bridge with kill switch enabled by default."

#include <Trade/Trade.mqh>

input string GenesisBaseUrl = "https://genesisbot-production.up.railway.app";
input bool AllowLiveTrading = false;
input bool DemoOnly = true;
input bool JournalOnly = true;
input double MaxRiskPct = 0.5;
input double MaxDailyLossPct = 2.0;
input int MaxOpenTrades = 1;
input int PollSeconds = 30;
input int MagicNumber = 50501;
input double MaxSpreadPoints = 50;
input string AllowedSymbols = "BTCUSD,NVDA,SPY,QQQ,XAUUSD";
input bool KillSwitch = true;

CTrade trade;
string lastDecision = "WAIT";
string lastConfidence = "low";
string lastReason = "waiting";
double lastRiskPct = 0.0;
double lastStop = 0.0;
double lastTarget = 0.0;
double lastHedgeScore = 0.0;
double lastNoTradeScore = 0.0;
datetime lastPoll = 0;
datetime lastTickSent = 0;
datetime lastJournalEvent = 0;
int lastSignalHttpCode = 0;
int lastTickHttpCode = 0;
int lastDecisionHttpCode = 0;
int lastAccountHttpCode = 0;
string lastSignalStatus = "never";
string lastTickStatus = "never";
string lastDecisionStatus = "never";
string lastAccountStatus = "never";
string lastError = "";
string lastResponseShort = "";

int OnInit()
{
   trade.SetExpertMagicNumber(MagicNumber);
   EventSetTimer(MathMax(PollSeconds, 5));
   Print("GenesisBridgeEA initialized. JournalOnly=", JournalOnly, " AllowLiveTrading=", AllowLiveTrading, " KillSwitch=", KillSwitch);
   SyncAccount();
   return(INIT_SUCCEEDED);
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   Comment("");
}

void OnTimer()
{
   if(!IsAllowedSymbol(_Symbol))
   {
      DrawPanel("NO_TRADE", "symbol_not_allowed");
      return;
   }
   SyncAccount();
   SendTick();
   PollDecision();
}

void PollDecision()
{
   string url = GenesisBaseUrl + "/api/genesis/mt5/decision?symbol=" + _Symbol;
   string response = GetJson(url, "decision", lastDecisionHttpCode, lastDecisionStatus);
   if(StringLen(response) <= 0)
   {
      DrawPanel("WAIT", "no_response_from_genesis");
      return;
   }

   lastDecision = JsonString(response, "decision", "WAIT");
   lastConfidence = JsonString(response, "confidence", "low");
   lastReason = JsonString(response, "reason", "no_reason");
   lastRiskPct = JsonNumber(response, "risk_pct", 0.0);
   lastStop = JsonNumber(response, "stop_loss", 0.0);
   lastTarget = JsonNumber(response, "take_profit", 0.0);
   lastHedgeScore = JsonNumber(response, "hedge_score", 0.0);
   lastNoTradeScore = JsonNumber(response, "no_trade_score", 0.0);
   lastPoll = TimeCurrent();

   DrawPanel(lastDecision, lastReason);
   SendSignalJournal(lastDecision, response);

   if(JournalOnly || !AllowLiveTrading || KillSwitch)
   {
      Print("Genesis journal only. Decision=", lastDecision, " reason=", lastReason);
      return;
   }

   if(DemoOnly && AccountInfoInteger(ACCOUNT_TRADE_MODE) != ACCOUNT_TRADE_MODE_DEMO)
   {
      Print("Blocked: DemoOnly=true and account is not demo.");
      return;
   }

   if(lastDecision == "BUY" || lastDecision == "SELL")
   {
      RequestOrderJournal(lastDecision);
      ExecuteDemoOrder(lastDecision);
   }
}

void SyncAccount()
{
   string payload = "{";
   payload += "\"symbol\":\"" + _Symbol + "\",";
   payload += "\"account_id\":\"" + IntegerToString((int)AccountInfoInteger(ACCOUNT_LOGIN)) + "\",";
   payload += "\"server\":\"" + AccountInfoString(ACCOUNT_SERVER) + "\",";
   payload += "\"currency\":\"" + AccountInfoString(ACCOUNT_CURRENCY) + "\",";
   payload += "\"balance\":" + DoubleToString(AccountInfoDouble(ACCOUNT_BALANCE), 2) + ",";
   payload += "\"equity\":" + DoubleToString(AccountInfoDouble(ACCOUNT_EQUITY), 2) + ",";
   payload += "\"margin\":" + DoubleToString(AccountInfoDouble(ACCOUNT_MARGIN), 2) + ",";
   payload += "\"free_margin\":" + DoubleToString(AccountInfoDouble(ACCOUNT_FREEMARGIN), 2) + ",";
   payload += "\"open_trades\":" + IntegerToString(PositionsTotal()) + ",";
   payload += "\"is_demo\":" + BoolJson(AccountInfoInteger(ACCOUNT_TRADE_MODE) == ACCOUNT_TRADE_MODE_DEMO) + ",";
   payload += "\"trade_mode\":\"" + TradeModeText() + "\",";
   payload += "\"broker_touched\":false";
   payload += "}";
   PostJson(GenesisBaseUrl + "/api/genesis/mt5/account-sync", payload, "account-sync", lastAccountHttpCode, lastAccountStatus);
}

void SendTick()
{
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double last = LastMarketPrice();
   string payload = "{";
   payload += "\"source\":\"mt5_bridge\",";
   payload += "\"symbol\":\"" + _Symbol + "\",";
   payload += "\"bid\":" + DoubleToString(bid, _Digits) + ",";
   payload += "\"ask\":" + DoubleToString(ask, _Digits) + ",";
   payload += "\"last\":" + DoubleToString(last, _Digits) + ",";
   payload += "\"spread\":" + DoubleToString(ask - bid, _Digits) + ",";
   payload += "\"spread_points\":" + DoubleToString(CurrentSpreadPoints(), 1) + ",";
   payload += "\"timeframe\":\"" + TimeframeText() + "\",";
   payload += "\"account\":\"" + IntegerToString((int)AccountInfoInteger(ACCOUNT_LOGIN)) + "\",";
   payload += "\"account_id\":\"" + IntegerToString((int)AccountInfoInteger(ACCOUNT_LOGIN)) + "\",";
   payload += "\"broker\":\"" + EscapeJson(AccountInfoString(ACCOUNT_COMPANY)) + "\",";
   payload += "\"server\":\"" + EscapeJson(AccountInfoString(ACCOUNT_SERVER)) + "\",";
   payload += "\"is_demo\":" + BoolJson(AccountInfoInteger(ACCOUNT_TRADE_MODE) == ACCOUNT_TRADE_MODE_DEMO) + ",";
   payload += "\"timestamp\":\"" + TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS) + "\",";
   payload += "\"broker_touched\":false,";
   payload += "\"order_executed\":false,";
   payload += "\"order_policy\":\"journal_only_no_broker\"";
   payload += "}";
   PostJson(GenesisBaseUrl + "/api/genesis/mt5/tick", payload, "tick", lastTickHttpCode, lastTickStatus);
   lastTickSent = TimeCurrent();
}

void SendSignalJournal(string decision, string rawDecision)
{
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double last = LastMarketPrice();
   double spread = ask - bid;
   string payload = "{";
   payload += "\"source\":\"mt5_bridge\",";
   payload += "\"event_type\":\"mt5_signal\",";
   payload += "\"symbol\":\"" + _Symbol + "\",";
   payload += "\"decision\":\"" + decision + "\",";
   payload += "\"timeframe\":\"" + TimeframeText() + "\",";
   payload += "\"price\":" + DoubleToString(last, _Digits) + ",";
   payload += "\"message\":\"MT5 bridge signal\",";
   payload += "\"confidence\":\"" + lastConfidence + "\",";
   payload += "\"reason\":\"" + EscapeJson(lastReason) + "\",";
   payload += "\"risk_pct\":" + DoubleToString(lastRiskPct, 4) + ",";
   payload += "\"stop_loss\":" + DoubleToString(lastStop, _Digits) + ",";
   payload += "\"take_profit\":" + DoubleToString(lastTarget, _Digits) + ",";
   payload += "\"hedge_score\":" + DoubleToString(lastHedgeScore, 0) + ",";
   payload += "\"no_trade_score\":" + DoubleToString(lastNoTradeScore, 0) + ",";
   payload += "\"journal_only\":" + BoolJson(JournalOnly) + ",";
   payload += "\"allow_live_trading\":" + BoolJson(AllowLiveTrading) + ",";
   payload += "\"kill_switch\":" + BoolJson(KillSwitch) + ",";
   payload += "\"payload\":{";
   payload += "\"symbol\":\"" + _Symbol + "\",";
   payload += "\"timeframe\":\"" + TimeframeText() + "\",";
   payload += "\"price\":" + DoubleToString(last, _Digits) + ",";
   payload += "\"bid\":" + DoubleToString(bid, _Digits) + ",";
   payload += "\"ask\":" + DoubleToString(ask, _Digits) + ",";
   payload += "\"spread\":" + DoubleToString(spread, _Digits) + ",";
   payload += "\"account\":\"" + IntegerToString((int)AccountInfoInteger(ACCOUNT_LOGIN)) + "\",";
   payload += "\"broker\":\"" + EscapeJson(AccountInfoString(ACCOUNT_COMPANY)) + "\",";
   payload += "\"server\":\"" + EscapeJson(AccountInfoString(ACCOUNT_SERVER)) + "\",";
   payload += "\"is_demo\":" + BoolJson(AccountInfoInteger(ACCOUNT_TRADE_MODE) == ACCOUNT_TRADE_MODE_DEMO);
   payload += "},";
   payload += "\"broker_touched\":false,";
   payload += "\"order_executed\":false,";
   payload += "\"order_policy\":\"journal_only_no_broker\"";
   payload += "}";
   PostJson(GenesisBaseUrl + "/api/genesis/mt5/signal", payload, "signal", lastSignalHttpCode, lastSignalStatus);
   lastJournalEvent = TimeCurrent();
}

void RequestOrderJournal(string decision)
{
   string payload = "{";
   payload += "\"source\":\"mt5_ea\",";
   payload += "\"symbol\":\"" + _Symbol + "\",";
   payload += "\"action\":\"" + decision + "\",";
   payload += "\"entry\":" + DoubleToString(SymbolInfoDouble(_Symbol, SYMBOL_BID), _Digits) + ",";
   payload += "\"stop_loss\":" + DoubleToString(lastStop, _Digits) + ",";
   payload += "\"take_profit\":" + DoubleToString(lastTarget, _Digits) + ",";
   payload += "\"risk_pct\":" + DoubleToString(lastRiskPct, 4) + ",";
   payload += "\"spread_points\":" + DoubleToString(CurrentSpreadPoints(), 1) + ",";
   payload += "\"confidence\":\"" + lastConfidence + "\",";
   payload += "\"hedge_score\":" + DoubleToString(lastHedgeScore, 0) + ",";
   payload += "\"no_trade_score\":" + DoubleToString(lastNoTradeScore, 0) + ",";
   payload += "\"broker_touched\":false";
   payload += "}";
   int orderRequestCode = 0;
   string orderRequestStatus = "";
   PostJson(GenesisBaseUrl + "/api/genesis/mt5/order-request", payload, "order-request", orderRequestCode, orderRequestStatus);
   lastJournalEvent = TimeCurrent();
}

void ExecuteDemoOrder(string decision)
{
   if(JournalOnly || !AllowLiveTrading || KillSwitch)
      return;
   if(DemoOnly && AccountInfoInteger(ACCOUNT_TRADE_MODE) != ACCOUNT_TRADE_MODE_DEMO)
   {
      Print("Blocked before CTrade: account is not demo.");
      return;
   }
   if(CurrentSpreadPoints() > MaxSpreadPoints)
   {
      Print("Blocked before CTrade: spread too high.");
      return;
   }
   if(PositionsTotal() >= MaxOpenTrades)
   {
      Print("Blocked before CTrade: max open trades reached.");
      return;
   }
   if(lastStop <= 0.0 || lastRiskPct <= 0.0 || lastRiskPct > MaxRiskPct)
   {
      Print("Blocked before CTrade: invalid stop or risk.");
      return;
   }

   double lots = SafeLotSize();
   bool sent = false;
   if(decision == "BUY")
      sent = trade.Buy(lots, _Symbol, 0.0, lastStop, lastTarget, "Genesis demo buy");
   if(decision == "SELL")
      sent = trade.Sell(lots, _Symbol, 0.0, lastStop, lastTarget, "Genesis demo sell");

   string resultPayload = "{";
   resultPayload += "\"source\":\"mt5_ea\",";
   resultPayload += "\"symbol\":\"" + _Symbol + "\",";
   resultPayload += "\"decision\":\"" + decision + "\",";
   resultPayload += "\"demo_only\":" + BoolJson(DemoOnly) + ",";
   resultPayload += "\"allow_live_trading\":" + BoolJson(AllowLiveTrading) + ",";
   resultPayload += "\"journal_only\":" + BoolJson(JournalOnly) + ",";
   resultPayload += "\"kill_switch\":" + BoolJson(KillSwitch) + ",";
   resultPayload += "\"sent\":" + BoolJson(sent) + ",";
   resultPayload += "\"retcode\":" + IntegerToString((int)trade.ResultRetcode()) + ",";
   resultPayload += "\"comment\":\"" + EscapeJson(trade.ResultComment()) + "\",";
   resultPayload += "\"broker_touched\":false,";
   resultPayload += "\"order_policy\":\"demo_only\"";
   resultPayload += "}";
   int orderResultCode = 0;
   string orderResultStatus = "";
   PostJson(GenesisBaseUrl + "/api/genesis/mt5/order-result", resultPayload, "order-result", orderResultCode, orderResultStatus);
   lastJournalEvent = TimeCurrent();
}

bool JsonToCharArray(string json, char &data[])
{
   ArrayResize(data, 0);
   int copied = StringToCharArray(json, data, 0, WHOLE_ARRAY, CP_UTF8);
   if(copied <= 0)
      return false;
   int size = copied;
   if(ArraySize(data) > 0 && data[ArraySize(data) - 1] == 0)
      size = copied - 1;
   if(size < 0)
      size = 0;
   ArrayResize(data, size);
   return true;
}

string GetJson(string url, string label, int &httpCode, string &status)
{
   char data[];
   char result[];
   string headers = "Content-Type: application/json\r\n";
   string resultHeaders = "";
   ResetLastError();
   int code = WebRequest("GET", url, headers, 5000, data, result, resultHeaders);
   httpCode = code;
   string response = CharArrayToString(result, 0, WHOLE_ARRAY, CP_UTF8);
   lastResponseShort = ShortText(response, 220);
   if(code < 200 || code >= 300)
   {
      status = "http_error";
      lastError = label + " GET HTTP " + IntegerToString(code) + " err=" + IntegerToString(GetLastError());
      Print("GET ", label, " failed code=", code, " err=", GetLastError(), " url=", url, " response=", lastResponseShort);
      return "";
   }
   status = "ok";
   return response;
}

string PostJson(string url, string payload, string label, int &httpCode, string &status)
{
   char data[];
   char result[];
   string headers = "Content-Type: application/json\r\n";
   string resultHeaders = "";
   if(!JsonToCharArray(payload, data))
   {
      httpCode = -1;
      status = "json_encode_error";
      lastError = label + " json_encode_error";
      Print("POST ", label, " failed before WebRequest: json_encode_error");
      return "";
   }
   ResetLastError();
   int code = WebRequest("POST", url, headers, 5000, data, result, resultHeaders);
   httpCode = code;
   string response = CharArrayToString(result, 0, WHOLE_ARRAY, CP_UTF8);
   lastResponseShort = ShortText(response, 220);
   if(code < 200 || code >= 300)
   {
      status = "http_error";
      lastError = label + " POST HTTP " + IntegerToString(code) + " err=" + IntegerToString(GetLastError());
      Print("POST ", label, " failed code=", code, " err=", GetLastError(), " url=", url, " response=", lastResponseShort);
      return "";
   }
   status = "ok";
   return response;
}

string HttpGet(string url)
{
   int code = 0;
   string status = "";
   return GetJson(url, "compat-get", code, status);
}

string HttpPost(string url, string payload)
{
   int code = 0;
   string status = "";
   return PostJson(url, payload, "compat-post", code, status);
}

void DrawPanel(string decision, string reason)
{
   string text = "Genesis MT5 Bridge\n";
   text += "Status: " + (KillSwitch ? "KILL SWITCH" : "ACTIVE") + "\n";
   text += "Decision: " + decision + "\n";
   text += "Confidence: " + lastConfidence + "\n";
   text += "Risk: " + DoubleToString(lastRiskPct, 2) + "%\n";
   text += "Stop: " + DoubleToString(lastStop, _Digits) + "\n";
   text += "Target: " + DoubleToString(lastTarget, _Digits) + "\n";
   text += "Hedge score: " + DoubleToString(lastHedgeScore, 0) + "\n";
   text += "No-trade score: " + DoubleToString(lastNoTradeScore, 0) + "\n";
   text += "Last tick sent: " + TimeLabel(lastTickSent) + "\n";
   text += "Last journal event: " + TimeLabel(lastJournalEvent) + "\n";
   text += "Last signal status: " + lastSignalStatus + "\n";
   text += "Last signal HTTP code: " + IntegerToString(lastSignalHttpCode) + "\n";
   text += "Last tick status: " + lastTickStatus + "\n";
   text += "Last tick HTTP code: " + IntegerToString(lastTickHttpCode) + "\n";
   text += "Last decision HTTP code: " + IntegerToString(lastDecisionHttpCode) + "\n";
   text += "Last error: " + ShortText(lastError, 80) + "\n";
   text += "Last response short: " + ShortText(lastResponseShort, 100) + "\n";
   text += "Broker touched: false\n";
   text += "Order executed: false\n";
   text += "JournalOnly: " + (JournalOnly ? "true" : "false") + "\n";
   text += "AllowLiveTrading: " + (AllowLiveTrading ? "true" : "false") + "\n";
   text += "Reason: " + reason;
   Comment(text);
}

bool IsAllowedSymbol(string symbol)
{
   string list = "," + AllowedSymbols + ",";
   return StringFind(list, "," + symbol + ",") >= 0;
}

double CurrentSpreadPoints()
{
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   if(_Point <= 0.0)
      return 0.0;
   return (ask - bid) / _Point;
}

double LastMarketPrice()
{
   double last = SymbolInfoDouble(_Symbol, SYMBOL_LAST);
   if(last > 0.0)
      return last;
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   if(bid > 0.0 && ask > 0.0)
      return (bid + ask) / 2.0;
   if(bid > 0.0)
      return bid;
   return ask;
}

double SafeLotSize()
{
   double minLot = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double maxLot = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
   double step = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   double lots = minLot;
   if(step > 0.0)
      lots = MathFloor(lots / step) * step;
   if(lots < minLot)
      lots = minLot;
   if(maxLot > 0.0 && lots > maxLot)
      lots = maxLot;
   return lots;
}

string TradeModeText()
{
   long mode = AccountInfoInteger(ACCOUNT_TRADE_MODE);
   if(mode == ACCOUNT_TRADE_MODE_DEMO)
      return "demo";
   if(mode == ACCOUNT_TRADE_MODE_REAL)
      return "real";
   if(mode == ACCOUNT_TRADE_MODE_CONTEST)
      return "contest";
   return "unknown";
}

string TimeframeText()
{
   if(_Period == PERIOD_M1) return "M1";
   if(_Period == PERIOD_M5) return "M5";
   if(_Period == PERIOD_M15) return "M15";
   if(_Period == PERIOD_M30) return "M30";
   if(_Period == PERIOD_H1) return "H1";
   if(_Period == PERIOD_H4) return "H4";
   if(_Period == PERIOD_D1) return "D1";
   if(_Period == PERIOD_W1) return "W1";
   if(_Period == PERIOD_MN1) return "MN1";
   return IntegerToString((int)_Period);
}

string TimeLabel(datetime value)
{
   if(value <= 0)
      return "never";
   return TimeToString(value, TIME_DATE|TIME_SECONDS);
}

string ShortText(string value, int maxLen)
{
   string clean = value;
   StringReplace(clean, "\r", " ");
   StringReplace(clean, "\n", " ");
   if(StringLen(clean) <= maxLen)
      return clean;
   return StringSubstr(clean, 0, maxLen);
}

string BoolJson(bool value)
{
   return value ? "true" : "false";
}

string EscapeJson(string value)
{
   string out = value;
   StringReplace(out, "\\", "\\\\");
   StringReplace(out, "\"", "\\\"");
   return out;
}

string JsonString(string json, string key, string fallback)
{
   string marker = "\"" + key + "\":";
   int pos = StringFind(json, marker);
   if(pos < 0)
      return fallback;
   int start = StringFind(json, "\"", pos + StringLen(marker));
   if(start < 0)
      return fallback;
   int finish = StringFind(json, "\"", start + 1);
   if(finish < 0)
      return fallback;
   return StringSubstr(json, start + 1, finish - start - 1);
}

double JsonNumber(string json, string key, double fallback)
{
   string marker = "\"" + key + "\":";
   int pos = StringFind(json, marker);
   if(pos < 0)
      return fallback;
   int start = pos + StringLen(marker);
   while(start < StringLen(json) && StringGetCharacter(json, start) == ' ')
      start++;
   int finish = start;
   while(finish < StringLen(json))
   {
      ushort ch = StringGetCharacter(json, finish);
      if((ch >= '0' && ch <= '9') || ch == '.' || ch == '-')
         finish++;
      else
         break;
   }
   if(finish <= start)
      return fallback;
   return StringToDouble(StringSubstr(json, start, finish - start));
}
