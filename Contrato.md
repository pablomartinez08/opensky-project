# 1. Motivación del Proyecto
## 1.1 Contexto y necesidad
El tráfico aéreo global genera decenas de miles de vuelos diarios, cada uno emitiendo señales de posición, altitud y velocidad en tiempo real a través de transpondedores ADS-B. Esta ingente cantidad de datos, disponible públicamente mediante plataformas como OpenSky Network, constituye una fuente de información de gran valor que actualmente no es explotada de forma sistemática para la detección temprana de anomalías operacionales.

Los sistemas de control de tráfico aéreo convencionales operan con herramientas diseñadas para la gestión individual de aeronaves, sin ofrecer una visión analítica global y automatizada sobre patrones anómalos que puedan afectar a zonas geográficas, aeropuertos o aeronaves concretas. La ausencia de este tipo de detección deja un vacío significativo en la capacidad de respuesta ante situaciones de riesgo potencial.

## 1.2 Patrones de comportamiento a detectar
El núcleo analítico del sistema se centra en la identificación de tres categorías de patrones anómalos:

a) Variaciones verticales bruscas
Se monitorizarán los cambios de altitud de cada aeronave a lo largo del tiempo. El sistema alertará cuando se detecte un ascenso o descenso que supere umbrales predefinidos en ventanas de tiempo cortas (del orden de 30 a 90 segundos), lo que puede indicar maniobras de emergencia, turbulencias severas o fallos en el sistema de navegación.

b) Anomalías asociadas a aeropuertos
El sistema correlacionará las alertas individuales con el aeropuerto de origen declarado por cada aeronave. Cuando múltiples aeronaves procedentes de un mismo aeropuerto presenten comportamientos anómalos en un intervalo de tiempo reducido, se generará una alerta de nivel superior que apuntará a una posible causa común de carácter operacional o meteorológico en dicho aeropuerto.

c) Zonas geográficas sin tráfico
Mediante la indexación del espacio aéreo con celdas hexagonales H3, el sistema identificará regiones geográficas que, teniendo históricamente tráfico aéreo regular, experimenten una ausencia prolongada de aeronaves. Este patrón puede ser indicativo de restricciones de espacio aéreo no previstas, condiciones meteorológicas extremas o incidentes de seguridad en dicha zona.

# 1.3 Objetivo general del proyecto
En resumen, el proyecto persigue construir un sistema funcional de análisis de tráfico aéreo en tiempo real que permita, de forma automatizada y sin intervención humana directa, identificar situaciones potencialmente anómalas en el espacio aéreo global. El resultado final será una herramienta de observabilidad que pueda ser utilizada tanto en contextos académicos como como base de una futura solución profesional orientada a organismos de control de tráfico aéreo, aerolíneas o autoridades de seguridad aérea.
