from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from app.settings import load_settings


def detect_weather_request(message: str) -> bool:
    text = str(message or "").casefold()
    return any(token in text for token in ("clima", "temperatura", "llueve", "lluvia", "weather", "tiempo", "viento", "pronostico", "pronóstico"))


def extract_city(message: str) -> str:
    text = str(message or "").strip()
    match = re.search(r"(?:en|de)\s+([\w\s.'-]{2,80})", text, flags=re.IGNORECASE | re.UNICODE)
    city = " ".join((match.group(1) if match else "").split()).strip(" ?.!").strip()
    city = re.sub(r"\b(hoy|ahora|por favor|favor)\b", "", city, flags=re.IGNORECASE).strip(" ,.")
    return city


def get_weather_answer(message: str) -> dict:
    city = extract_city(message)
    settings = load_settings()
    if not city:
        return {
            "ok": False,
            "intent": "weather",
            "city": "",
            "answer": "Dime la ciudad para consultar el clima real.",
            "source": "weather_missing_city",
        }
    if not settings.weather_api_key:
        return _open_meteo_answer(city)
    try:
        weather = _fetch_openweather(city, settings.weather_api_key)
    except Exception as exc:
        return {
            "ok": False,
            "intent": "weather",
            "city": city,
            "answer": f"No pude consultar el clima real de {city}. La clave existe, pero el proveedor devolvio error o no hubo conexion.",
            "source": "openweather_error",
            "error": str(exc)[:160],
        }
    temp = weather.get("temp") or 0.0
    feels = weather.get("feels_like") or 0.0
    humidity = weather.get("humidity") or 0.0
    wind = (weather.get("wind_speed") or 0.0) * 3.6
    description = weather.get("description") or "sin descripcion"
    place = weather.get("name") or city
    icon = _weather_icon(weather.get("weather_code"), description)
    updated_at = _timestamp_to_iso(weather.get("timestamp"))
    answer = (
        f"Clima en {place}: {description}. "
        f"Temperatura {temp:.1f} C, sensacion {feels:.1f} C, humedad {humidity:.0f}% "
        f"y viento {wind:.1f} km/h. Fuente: OpenWeather."
    )
    return {
        "ok": True,
        "intent": "weather",
        "city": place,
        "answer": answer,
        "source": "openweather",
        "condition": description,
        "weather_code": weather.get("weather_code"),
        "icon": icon,
        "temperature": temp,
        "feels_like": feels,
        "min_temp": weather.get("min_temp"),
        "max_temp": weather.get("max_temp"),
        "precipitation_probability": weather.get("rain_probability"),
        "wind_speed": wind,
        "updated_at": updated_at,
        "data": weather,
    }


def _open_meteo_answer(city: str) -> dict:
    try:
        place = _geocode_open_meteo(city)
        if not place:
            return {
                "ok": False,
                "intent": "weather",
                "city": city,
                "answer": "No pude confirmar el clima de esa ubicacion. Dime ciudad y estado para buscarlo mejor.",
                "source": "open_meteo_geocoding_empty",
            }
        weather = _fetch_open_meteo_weather(place)
    except Exception as exc:
        return {
            "ok": False,
            "intent": "weather",
            "city": city,
            "answer": "No pude confirmar el clima de esa ubicacion. Dime ciudad y estado para buscarlo mejor.",
            "source": "open_meteo_error",
            "error": str(exc)[:160],
        }
    name = place.get("name") or city
    admin = place.get("admin1") or ""
    country = place.get("country") or ""
    label = ", ".join(part for part in (name, admin, country) if part)
    temp = weather.get("temperature")
    high = weather.get("max_temp")
    low = weather.get("min_temp")
    rain = weather.get("rain_probability")
    wind = weather.get("wind_speed")
    condition = weather.get("condition") or "condicion no especificada"
    icon = _weather_icon(weather.get("weather_code"), condition, bool(weather.get("is_day", True)))
    answer = (
        f"En {label} esta {condition}, cerca de {temp:.1f} C. "
        f"Rango esperado {low:.1f} C a {high:.1f} C, lluvia {rain:.0f}% y viento {wind:.1f} km/h. "
        "Fuente: Open-Meteo."
    )
    return {
        "ok": True,
        "intent": "weather",
        "city": label,
        "answer": answer,
        "source": "open_meteo",
        "condition": condition,
        "weather_code": weather.get("weather_code"),
        "icon": icon,
        "temperature": temp,
        "feels_like": weather.get("feels_like"),
        "min_temp": low,
        "max_temp": high,
        "precipitation_probability": rain,
        "wind_speed": wind,
        "updated_at": weather.get("updated_at"),
        "data": {**weather, "place": place},
    }


def _fetch_openweather(city: str, api_key: str) -> dict:
    query = urllib.parse.urlencode({"q": city, "appid": api_key, "units": "metric", "lang": "es"})
    request = urllib.request.Request(f"https://api.openweathermap.org/data/2.5/weather?{query}", method="GET")
    with urllib.request.urlopen(request, timeout=8) as response:
        payload = json.loads(response.read().decode("utf-8"))
    weather = (payload.get("weather") or [{}])[0] if isinstance(payload, dict) else {}
    main = payload.get("main") or {}
    wind = payload.get("wind") or {}
    return {
        "name": payload.get("name") or city,
        "description": weather.get("description") or weather.get("main") or "",
        "weather_code": weather.get("id"),
        "temp": _num(main.get("temp")),
        "feels_like": _num(main.get("feels_like")),
        "min_temp": _num(main.get("temp_min")),
        "max_temp": _num(main.get("temp_max")),
        "humidity": _num(main.get("humidity")),
        "wind_speed": _num(wind.get("speed")),
        "rain_probability": None,
        "timestamp": payload.get("dt"),
    }


def _geocode_open_meteo(city: str) -> dict | None:
    query = urllib.parse.urlencode({"name": city, "count": 1, "language": "es", "format": "json"})
    request = urllib.request.Request(f"https://geocoding-api.open-meteo.com/v1/search?{query}", method="GET")
    with urllib.request.urlopen(request, timeout=8) as response:
        payload = json.loads(response.read().decode("utf-8"))
    results = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results:
        return None
    first = results[0] if isinstance(results[0], dict) else {}
    if first.get("latitude") is None or first.get("longitude") is None:
        return None
    return {
        "name": first.get("name") or city,
        "admin1": first.get("admin1") or "",
        "country": first.get("country") or "",
        "latitude": _num(first.get("latitude")),
        "longitude": _num(first.get("longitude")),
    }


def _fetch_open_meteo_weather(place: dict) -> dict:
    query = urllib.parse.urlencode(
        {
            "latitude": place["latitude"],
            "longitude": place["longitude"],
            "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,rain,weather_code,wind_speed_10m,is_day",
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_probability_max",
            "timezone": "auto",
        }
    )
    request = urllib.request.Request(f"https://api.open-meteo.com/v1/forecast?{query}", method="GET")
    with urllib.request.urlopen(request, timeout=8) as response:
        payload = json.loads(response.read().decode("utf-8"))
    current = payload.get("current") or {}
    daily = payload.get("daily") or {}
    code = int(_num(current.get("weather_code")))
    return {
        "temperature": _num(current.get("temperature_2m")),
        "feels_like": _num(current.get("apparent_temperature")),
        "humidity": _num(current.get("relative_humidity_2m")),
        "precipitation": _num(current.get("precipitation")),
        "rain": _num(current.get("rain")),
        "weather_code": code,
        "condition": _weather_code_label(code),
        "icon": _weather_icon(code, _weather_code_label(code), bool(_num(current.get("is_day")) or 0)),
        "is_day": bool(_num(current.get("is_day")) or 0),
        "wind_speed": _num(current.get("wind_speed_10m")),
        "max_temp": _first_num(daily.get("temperature_2m_max")),
        "min_temp": _first_num(daily.get("temperature_2m_min")),
        "rain_probability": _first_num(daily.get("precipitation_probability_max")),
        "updated_at": current.get("time") or "",
    }


def _weather_code_label(code: int) -> str:
    if code == 0:
        return "despejado"
    if code in {1, 2}:
        return "parcialmente nublado"
    if code == 3:
        return "nublado"
    if code in {45, 48}:
        return "con niebla"
    if code in {51, 53, 55, 56, 57}:
        return "con llovizna"
    if code in {61, 63, 65, 66, 67, 80, 81, 82}:
        return "con lluvia"
    if code in {71, 73, 75, 77, 85, 86}:
        return "con nieve"
    if code in {95, 96, 99}:
        return "con tormenta"
    return "sin condicion confirmada"


def _weather_icon(code: object, condition: str = "", is_day: bool = True) -> str:
    numeric = int(_num(code))
    text = str(condition or "").casefold()
    if numeric == 0:
        return "\u2600\ufe0f" if is_day else "\U0001f319"
    if numeric in {1, 2}:
        return "\u26c5"
    if numeric == 3 or "nublado" in text:
        return "\u2601\ufe0f"
    if numeric in {45, 48} or "niebla" in text:
        return "\U0001f32b\ufe0f"
    if numeric in {51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82} or "lluvia" in text or "llovizna" in text:
        return "\U0001f327\ufe0f"
    if numeric in {95, 96, 99} or "tormenta" in text:
        return "\u26c8\ufe0f"
    if "viento" in text:
        return "\U0001f4a8"
    if "claro" in text or "despejado" in text:
        return "\u2600\ufe0f" if is_day else "\U0001f319"
    return "\u2601\ufe0f"


def _timestamp_to_iso(value: object) -> str:
    try:
        numeric = float(value)
        if numeric <= 0:
            return ""
        return datetime.fromtimestamp(numeric, tz=timezone.utc).isoformat()
    except Exception:
        return ""


def _first_num(value: object) -> float:
    if isinstance(value, list) and value:
        return _num(value[0])
    return _num(value)


def _num(value: object) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0
