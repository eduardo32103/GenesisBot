# Modelo de Trabajo por Roles - Génesis

## Objetivo
Mantener el trabajo de Génesis ordenado por subcortes pequeños, con dueño claro, validación clara y fases cerradas congeladas para evitar mezclar frentes.

## Roles Base

### 1. Coordinador
- Responsabilidad: definir el subcorte actual, decidir qué rol entra, evitar mezclar frentes, congelar fases cerradas y exigir validación antes del cierre.
- Sí toca: alcance, orden, criterio de cierre, dependencias entre subcortes.
- No toca: implementación detallada salvo ajustes mínimos de coordinación.

### 2. Runtime / Bot
- Responsabilidad: Telegram, handlers, boot, polling, background y estabilidad del baseline.
- Sí toca: `main.py` en handlers, arranque, loop, integración con comandos y rutas activas del bot.
- No toca: diseño del dashboard, decisiones de capacidad del proveedor, frontend.

### 3. Datos / FMP
- Responsabilidad: FMP, caché, throttle, cooldown, clasificación de errores, consumo por endpoint y capacidad del proveedor.
- Sí toca: integración con FMP, políticas de lectura, métricas de consumo y diagnóstico del proveedor.
- No toca: layout del dashboard, UX, lógica visual del frontend.

### 4. Dashboard Backend
- Responsabilidad: endpoints del dashboard, view models, servicios de lectura y reutilización de DB, memoria y reportes existentes.
- Sí toca: agregación de datos, shape de respuesta, composición de módulos del dashboard.
- No toca: polling del bot, lógica de FMP de bajo nivel, diseño visual final.

### 5. Dashboard Frontend
- Responsabilidad: layout, navegación, tarjetas, tablas y UX del dashboard.
- Sí toca: estructura visual, jerarquía de información, interacción de la UI.
- No toca: fuentes de datos, caché FMP, lógica operativa del bot.

### 6. QA / Validación
- Responsabilidad: checklist, smoke tests, regresiones, criterio de cierre y evidencia en logs.
- Sí toca: validación final, evidencia de Telegram, Railway y comportamiento observable.
- No toca: ampliar alcance del subcorte ni mezclar cambios nuevos durante la validación.

### 7. Documentación / Memoria Operativa
- Responsabilidad: notas de causa raíz, roadmap actualizado, baseline congelado y registro de qué está cerrado y qué no se debe tocar.
- Sí toca: `docs/`, notas de cierre, baseline y acuerdos operativos.
- No toca: lógica funcional salvo documentación de impacto.

## Reglas de Colaboración
- No reabrir fases cerradas sin motivo puntual.
- Un subcorte a la vez.
- Un dueño principal por subcorte.
- QA siempre valida al final.
- No mezclar bot, dashboard, FMP y frontend en el mismo cambio salvo necesidad clara.
- No tocar el baseline estable por “aprovechar de una vez”.
- Documentar cierres importantes.

## Flujo de Trabajo por Subcorte
1. Coordinador define el subcorte.
2. Se asigna un dueño principal.
3. Solo entran los roles estrictamente necesarios.
4. El dueño implementa el cambio mínimo.
5. QA valida con checklist y evidencia real.
6. Documentación registra el cierre y congela el bloque.
7. Solo entonces se abre el siguiente subcorte.

## Aplicación Inmediata a Fase 3

### Fase 3.1 - Shell Dashboard
- Dueño principal: Dashboard Frontend.
- Soporte: Dashboard Backend.
- QA valida: carga básica, estructura inicial y ausencia de regresiones.
- Documentación registra: apertura formal de Fase 3 y alcance del shell.

### Fase 3.2 - Salud Operativa
- Dueño principal: Runtime / Bot.
- Soporte: Dashboard Backend.
- QA valida: estado de boot, liderazgo, polling, baseline y degradación visible.
- Documentación registra: qué métricas operativas quedaron expuestas.

### Fase 3.3 - Radar / Cartera
- Dueño principal: Dashboard Backend.
- Soporte: Runtime / Bot.
- QA valida: coherencia con `Mi Cartera`, radar y etiquetas de caché.
- Documentación registra: qué fuentes alimentan el módulo.

### Fase 3.4 - Alertas
- Dueño principal: Dashboard Backend.
- Soporte: Runtime / Bot.
- QA valida: consistencia con reportes y score existentes.
- Documentación registra: qué métricas del motor ya están visibles.

### Fase 3.5 - FMP / Dependencias
- Dueño principal: Datos / FMP.
- Soporte: Dashboard Backend.
- QA valida: lectura correcta de `quota`, `access`, `cache hit`, `throttle` y `blocked`.
- Documentación registra: criterio operativo de capacidad visible desde el dashboard.

### Fase 3.6 - Macro / Actividad
- Dueño principal: Dashboard Backend.
- Soporte: Datos / FMP y Runtime / Bot solo si hace falta.
- QA valida: coherencia con snapshot macro y actividad reciente.
- Documentación registra: módulos ya cerrados de Fase 3.

## Orden Práctico de Ejecución
1. Coordinador abre el subcorte.
2. Entra el dueño principal.
3. Entra el rol de soporte solo si el dueño lo necesita.
4. QA valida al final.
5. Documentación congela el cierre.

## Regla Simple para Evitar Caos
- Si el cambio es de estado del bot, entra Runtime / Bot.
- Si el cambio es de proveedor o consumo FMP, entra Datos / FMP.
- Si el cambio es de lectura/composición del dashboard, entra Dashboard Backend.
- Si el cambio es visual, entra Dashboard Frontend.
- Si el cambio ya funciona pero falta evidencia, entra QA / Validación.
- Si el cambio ya cerró y hay que dejar memoria, entra Documentación / Memoria Operativa.

## Baseline
- Fase 2.1 a 2.6 quedan congeladas como baseline estable.
- No deben tocarse salvo bug puntual.

## Extension Fase 4

### Fase 4.1 - Drilldown por ticker (web)
- DueÃ±o principal: Dashboard Frontend.
- Soporte: Dashboard Backend.
- QA valida: apertura del detalle real desde `Radar / Cartera`, ficha tÃ¡ctica con datos reales y baseline del dashboard intacto.
- DocumentaciÃ³n registra: endpoint del drilldown y superficie oficial del dashboard web en `app/dashboard/`.
