//+------------------------------------------------------------------+
//| GenesisBridgeEA.mq5                                             |
//| Genesis MT5 Bridge - demo/backtest/journal first.                |
//| No real trading by default.                                      |
//+------------------------------------------------------------------+
#property strict
#property version   "11.9"
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
input string AllowedSymbols = "BTC,BTCUSD,NVDA,SPY,QQQ,XAUUSD";
input bool KillSwitch = false;
input bool EnableTickPost = true;
input bool EnableSignalPost = false;
input bool EnableAccountSync = true;
input bool EnableDecisionPoll = true;

CTrade trade;
string EA_VERSION = "GenesisBridgeEA_v11_9_FORCE_TICK";
string lastDecision = "WAIT";
string lastConfidence = "low";
string lastReason = "waiting";
string lastDecisionRawResponse = "";
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
string lastTickError = "";
string lastResponseShort = "";

int OnInit()
{
   trade.SetExpertMagicNumber(MagicNumber);
   EventSetTimer(MathMax(PollSeconds, 5));
   Print("GenesisBridgeEA initialized. JournalOnly=", JournalOnly, " AllowLiveTrading=", AllowLiveTrading, " KillSwitch=", KillSwitch);
   if(EnableAccountSync)
      SendAccountSync();
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
      Print("MT5 allowed-symbol warning: ", _Symbol, " not in AllowedSymbols, but account-sync/tick/decision will still run.");
   if(EnableAccountSync)
      SendAccountSync();
   else
      lastAccountStatus = "disabled";

   if(EnableTickPost)
   {
      bool tickSent = SendTick();
      if(!tickSent)
         Print("MT5 tick warning: SendTick returned false but decision poll will continue.");
   }
   else
   {
      lastTickStatus = "disabled";
      Print("MT5 SendTick disabled by EnableTickPost=false");
   }

   if(EnableDecisionPoll)
      PollDecision();
   else
      lastDecisionStatus = "disabled";

   if(EnableSignalPost)
      SendSignalJournal(lastDecision, lastDecisionRawResponse);
   else
      lastSignalStatus = "disabled";
}

void PollDecision()
{
   string response = "";
   lastDecisionHttpCode = GetJson("/api/genesis/mt5/decision?symbol=" + _Symbol, response);
   lastDecisionStatus = HttpStatusText(lastDecisionHttpCode);
   lastDecisionRawResponse = response;
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

bool SendAccountSync()
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
   string response = "";
   lastAccountHttpCode = PostJson("/api/genesis/mt5/account-sync", payload, response);
   lastAccountStatus = HttpStatusText(lastAccountHttpCode);
   return (lastAccountHttpCode >= 200 && lastAccountHttpCode < 300);
}

void SyncAccount()
{
   SendAccountSync();
}

bool SendTick()
{
   Print("MT5 SendTick start symbol=", _Symbol, " tf=", TimeframeText());
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double last = LastMarketPrice();
   if(last <= 0.0 && bid <= 0.0 && ask <= 0.0)
   {
      lastTickHttpCode = -2;
      lastTickStatus = "no_price";
      lastTickError = "tick skipped: no price";
      Print("MT5 SendTick local error=tick skipped: no price");
      return false;
   }
   if(bid <= 0.0)
      bid = last;
   if(ask <= 0.0)
      ask = last;
   if(last <= 0.0)
      last = (bid + ask) / 2.0;
   string payload = "{";
   payload += "\"symbol\":\"" + _Symbol + "\",";
   payload += "\"bid\":" + DoubleToString(bid, _Digits) + ",";
   payload += "\"ask\":" + DoubleToString(ask, _Digits) + ",";
   payload += "\"last\":" + DoubleToString(last, _Digits) + ",";
   payload += "\"spread\":" + DoubleToString(ask - bid, _Digits) + ",";
   payload += "\"timeframe\":\"" + TimeframeText() + "\",";
   payload += "\"account\":\"" + IntegerToString((int)AccountInfoInteger(ACCOUNT_LOGIN)) + "\",";
   payload += "\"broker\":\"" + EscapeJson(AccountInfoString(ACCOUNT_COMPANY)) + "\",";
   payload += "\"server\":\"" + EscapeJson(AccountInfoString(ACCOUNT_SERVER)) + "\",";
   payload += "\"is_demo\":" + BoolJson(AccountInfoInteger(ACCOUNT_TRADE_MODE) == ACCOUNT_TRADE_MODE_DEMO) + ",";
   payload += "\"source\":\"mt5_bridge\",";
   payload += "\"ea_version\":\"" + EA_VERSION + "\"";
   payload += "}";
   Print("MT5 SendTick JSON=", ShortText(payload, 350));
   string response = "";
   lastTickHttpCode = PostJson("/api/genesis/mt5/tick", payload, response);
   lastTickStatus = HttpStatusText(lastTickHttpCode);
   lastTickError = "";
   Print("MT5 SendTick HTTP=", lastTickHttpCode);
   Print("MT5 SendTick response=", ShortText(response, 350));
   if(lastTickHttpCode < 200 || lastTickHttpCode >= 300)
   {
      lastTickError = "HTTP " + IntegerToString(lastTickHttpCode) + " err=" + IntegerToString(GetLastError());
      Print("MT5 SendTick error=", GetLastError());
      Print("MT5 SendTick response body=", ShortText(response, 500));
   }
   lastTickSent = TimeCurrent();
   return (lastTickHttpCode >= 200 && lastTickHttpCode < 300);
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
   string response = "";
   lastSignalHttpCode = PostJson("/api/genesis/mt5/signal", payload, response);
   lastSignalStatus = HttpStatusText(lastSignalHttpCode);
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
   string response = "";
   PostJson("/api/genesis/mt5/order-request", payload, response);
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
   string response = "";
   PostJson("/api/genesis/mt5/order-result", resultPayload, response);
   lastJournalEvent = TimeCurrent();
}

bool StringToUtf8Body(string text, char &body[])
{
   ArrayResize(body, 0);
   int copied = StringToCharArray(text, body, 0, WHOLE_ARRAY, CP_UTF8);
   if(copied <= 0)
      return false;
   int size = copied;
   if(ArraySize(body) > 0 && body[ArraySize(body) - 1] == 0)
      size = copied - 1;
   if(size < 0)
      size = 0;
   ArrayResize(body, size);
   return true;
}

bool JsonToCharArray(string json, char &data[])
{
   return StringToUtf8Body(json, data);
}

string EndpointUrl(string path)
{
   if(StringFind(path, "http://") == 0 || StringFind(path, "https://") == 0)
      return path;
   if(StringLen(path) > 0 && StringSubstr(path, 0, 1) == "/")
      return GenesisBaseUrl + path;
   return GenesisBaseUrl + "/" + path;
}

int GetJson(string path, string &response)
{
   char data[];
   char result[];
   string headers = "Content-Type: application/json\r\n";
   string resultHeaders = "";
   string url = EndpointUrl(path);
   response = "";
   ResetLastError();
   int code = WebRequest("GET", url, headers, 5000, data, result, resultHeaders);
   response = CharArrayToString(result, 0, WHOLE_ARRAY, CP_UTF8);
   lastResponseShort = ShortText(response, 220);
   if(code < 200 || code >= 300)
   {
      lastError = "GET HTTP " + IntegerToString(code) + " err=" + IntegerToString(GetLastError());
      Print("GET failed code=", code, " err=", GetLastError(), " url=", url, " response=", lastResponseShort);
      return code;
   }
   return code;
}

int PostJson(string path, string json, string &response)
{
   char data[];
   char result[];
   string headers = "Content-Type: application/json\r\n";
   string resultHeaders = "";
   string url = EndpointUrl(path);
   response = "";
   if(!JsonToCharArray(json, data))
   {
      lastError = "json_encode_error";
      Print("POST failed before WebRequest: json_encode_error url=", url);
      return -1;
   }
   ResetLastError();
   int code = WebRequest("POST", url, headers, 5000, data, result, resultHeaders);
   response = CharArrayToString(result, 0, WHOLE_ARRAY, CP_UTF8);
   lastResponseShort = ShortText(response, 220);
   if(code < 200 || code >= 300)
   {
      lastError = "POST HTTP " + IntegerToString(code) + " err=" + IntegerToString(GetLastError());
      Print("POST failed code=", code, " err=", GetLastError(), " url=", url, " response=", lastResponseShort);
      return code;
   }
   return code;
}

string HttpGet(string url)
{
   string response = "";
   GetJson(url, response);
   return response;
}

string HttpPost(string url, string payload)
{
   string response = "";
   PostJson(url, payload, response);
   return response;
}

string HttpStatusText(int code)
{
   if(code >= 200 && code < 300)
      return "ok";
   if(code == -1)
      return "json_encode_error";
   if(code == -2)
      return "not_sent";
   return "http_error";
}

void DrawPanel(string decision, string reason)
{
   string text = "Genesis MT5 Bridge\n";
   text += "EA version: " + EA_VERSION + "\n";
   text += "Status: " + (KillSwitch ? "KILL SWITCH" : "ACTIVE") + "\n";
   text += "Decision: " + decision + "\n";
   text += "Confidence: " + lastConfidence + "\n";
   text += "Risk: " + DoubleToString(lastRiskPct, 2) + "%\n";
   text += "Stop: " + DoubleToString(lastStop, _Digits) + "\n";
   text += "Target: " + DoubleToString(lastTarget, _Digits) + "\n";
   text += "Hedge score: " + DoubleToString(lastHedgeScore, 0) + "\n";
   text += "No-trade score: " + DoubleToString(lastNoTradeScore, 0) + "\n";
   text += "LastTickTime: " + TimeLabel(lastTickSent) + "\n";
   text += "Last journal event: " + TimeLabel(lastJournalEvent) + "\n";
   text += "Last signal status: " + lastSignalStatus + "\n";
   text += "LastSignalHttpCode: " + IntegerToString(lastSignalHttpCode) + "\n";
   text += "Last tick status: " + lastTickStatus + "\n";
   text += "LastTickHttpCode: " + IntegerToString(lastTickHttpCode) + "\n";
   text += "LastTickError: " + ShortText(lastTickError, 80) + "\n";
   text += "LastDecisionHttpCode: " + IntegerToString(lastDecisionHttpCode) + "\n";
   text += "LastAccountSyncHttpCode: " + IntegerToString(lastAccountHttpCode) + "\n";
   text += "Last error: " + ShortText(lastError, 80) + "\n";
   text += "Last response short: " + ShortText(lastResponseShort, 100) + "\n";
   text += "Broker touched: false\n";
   text += "Order executed: false\n";
   text += "JournalOnly: " + (JournalOnly ? "true" : "false") + "\n";
   text += "AllowLiveTrading: " + (AllowLiveTrading ? "true" : "false") + "\n";
   text += "DemoOnly: " + (DemoOnly ? "true" : "false") + "\n";
   text += "KillSwitch: " + (KillSwitch ? "true" : "false") + "\n";
   text += "EnableTickPost: " + (EnableTickPost ? "true" : "false") + "\n";
   text += "EnableSignalPost: " + (EnableSignalPost ? "true" : "false") + "\n";
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
