# 🌤️ Weather ETL Pipeline

[![Java](https://img.shields.io/badge/Java-17-orange)](https://openjdk.org/projects/jdk/17/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.0-brightgreen)](https://spring.io/projects/spring-boot)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)](https://www.docker.com/)
[![Gradle](https://img.shields.io/badge/Gradle-8.5-02303A)](https://gradle.org/)

ETL (Extract, Transform, Load) пайплайн для обработки данных прогнозов погоды с использованием [Open-Meteo API](https://open-meteo.com/).

## 📋 Содержание

- [Особенности](#особенности)
- [Архитектура](#архитектура)
- [Быстрый старт](#быстрый-старт)
- [Установка](#установка)
- [Использование](#использование)
- [API Documentation](#api-documentation)
- [Конфигурация](#конфигурация)
- [Docker](#docker)
- [Разработка](#разработка)
- [Тестирование](#тестирование)
- [Мониторинг](#мониторинг)

## ✨ Особенности

### 🏗️ Архитектурные решения
- **Современная архитектура** на базе Spring Boot 3.x
- **Реактивное программирование** с WebFlux для HTTP клиента
- **Микросервисная архитектура** готова к горизонтальному масштабированию
- **Паттерны устойчивости**: Circuit Breaker, Retry, Timeout

### 📊 Обработка данных
- **Извлечение данных** из Open-Meteo API
- **Трансформация**: преобразование единиц измерения, агрегация данных
- **Загрузка**: экспорт в CSV и сохранение в PostgreSQL
- **Обработка дубликатов**: уникальные ограничения и upsert операции

### 🔧 Функциональность
- **Command Line Interface** для автоматизации
- **REST API** для интеграции с другими системами
- **Веб-интерфейс** для мониторинга и управления
- **Гибкая конфигурация** через application.yml

### 🛠️ DevOps готовность
- **Контейнеризация** с Docker и Docker Compose
- **Мониторинг** через Spring Boot Actuator
- **Логирование** с настраиваемыми уровнями
- **Health checks** для проверки состояния сервисов

## 🏛️ Архитектура

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Open-Meteo    │    │  Weather ETL    │    │   PostgreSQL    │
│      API        │───▶│   Application   │───▶│    Database     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   CSV Export    │
                       └─────────────────┘
```

### Компоненты системы

1. **WeatherApiClient** - HTTP клиент для работы с Open-Meteo API
2. **WeatherTransformer** - сервис трансформации данных
3. **CsvExportService** - экспорт данных в CSV формат
4. **WeatherDatabaseService** - работа с базой данных
5. **WeatherEtlService** - оркестрация ETL процесса
6. **WeatherEtlController** - REST API контроллер
7. **WeatherEtlCli** - интерфейс командной строки

## 🚀 Быстрый старт

### Предварительные требования

- Java 17 или выше
- Docker и Docker Compose
- PostgreSQL 15+ (при локальной разработке)

### Запуск с Docker Compose

```bash
# Клонирование репозитория
git clone https://github.com/your-username/weather-etl-pipeline.git
cd weather-etl-pipeline

# Сборка и запуск всех сервисов
docker-compose up -d

# Проверка состояния сервисов
docker-compose ps

# Просмотр логов
docker-compose logs -f weather-etl
```

### Базовое использование

```bash
# Извлечение данных из API и сохранение в CSV
curl -X POST "http://localhost:8080/api/v1/weather-etl/execute/api-to-csv" \
  -G -d "startDate=2025-05-16" -d "endDate=2025-05-30"

# Извлечение данных из API и сохранение в базу данных
curl -X POST "http://localhost:8080/api/v1/weather-etl/execute/api-to-database" \
  -G -d "startDate=2025-05-16" -d "endDate=2025-05-30"
```

## 📦 Установка

### Локальная разработка

1. **Клонирование репозитория**
```bash
git clone https://github.com/your-username/weather-etl-pipeline.git
cd weather-etl-pipeline
```

2. **Настройка базы данных**
```bash
# Запуск только PostgreSQL
docker-compose up -d postgres

# Или установка PostgreSQL локально
# См. https://www.postgresql.org/download/
```

3. **Настройка конфигурации**
```bash
# Копирование и настройка конфигурации
cp src/main/resources/application.yml src/main/resources/application-local.yml
# Отредактируйте настройки подключения к базе данных
```

4. **Сборка и запуск**
```bash
# Сборка проекта
./gradlew clean build

# Запуск приложения
./gradlew bootRun --args="--spring.profiles.active=local"
```

### Продакшн развертывание

```bash
# Сборка JAR файла
./gradlew clean bootJar

# Запуск приложения
java -jar build/libs/weather-etl-pipeline-*.jar \
  --spring.profiles.active=production \
  --spring.datasource.url=jdbc:postgresql://your-db-host:5432/weather_db \
  --spring.datasource.username=weather_user \
  --spring.datasource.password=your_password
```

## 🔧 Использование

### Command Line Interface

#### Основные команды

```bash
# Справка по использованию
java -jar weather-etl-pipeline.jar --help

# Извлечение данных из API и сохранение в CSV
java -jar weather-etl-pipeline.jar \
  --source=api \
  --output=csv \
  --start-date=2025-05-16 \
  --end-date=2025-05-30

# Извлечение данных из API и сохранение в базу данных
java -jar weather-etl-pipeline.jar \
  --source=api \
  --output=database \
  --start-date=2025-05-16 \
  --end-date=2025-05-30

# Извлечение данных из API и сохранение в CSV + базу данных
java -jar weather-etl-pipeline.jar \
  --source=api \
  --output=all \
  --start-date=2025-05-16 \
  --end-date=2025-05-30 \
  --csv-path=/custom/path/weather.csv
```

#### Обработка JSON файлов

```bash
# Обработка JSON файла и сохранение в CSV
java -jar weather-etl-pipeline.jar \
  --source=json \
  --output=csv \
  --json-path=/path/to/weather-data.json

# Обработка JSON файла и сохранение в базу данных
java -jar weather-etl-pipeline.jar \
  --source=json \
  --output=database \
  --json-path=/path/to/weather-data.json
```

### Docker команды

```bash
# Выполнение ETL процесса в Docker контейнере
docker run --rm --network=weather-network weather-etl:latest \
  --source=api \
  --output=csv \
  --start-date=2025-05-16 \
  --end-date=2025-05-30

# Выполнение с подключением к базе данных
docker run --rm --network=weather-network weather-etl:latest \
  --source=api \
  --output=database \
  --start-date=2025-05-16 \
  --end-date=2025-05-30

# Выполнение с монтированием томов для CSV файлов
docker run --rm \
  --network=weather-network \
  -v $(pwd)/output:/app/output \
  weather-etl:latest \
  --source=api \
  --output=csv \
  --start-date=2025-05-16 \
  --end-date=2025-05-30
```

## 📚 API Documentation

### REST Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/weather-etl/execute/api-to-csv` | POST | Извлечение из API → CSV |
| `/api/v1/weather-etl/execute/api-to-database` | POST | Извлечение из API → Database |
| `/api/v1/weather-etl/execute/api-to-all` | POST | Извлечение из API → CSV + Database |
| `/api/v1/weather-etl/stats` | GET | Статистика ETL процесса |
| `/api/v1/weather-etl/health` | GET | Проверка состояния сервиса |

### Примеры запросов

```bash
# GET статистики
curl -X GET "http://localhost:8080/api/v1/weather-etl/stats"

# POST запрос на выполнение ETL
curl -X POST "http://localhost:8080/api/v1/weather-etl/execute/api-to-csv" \
  -G \
  -d "startDate=2025-05-16" \
  -d "endDate=2025-05-30" \
  -d "csvPath=/custom/path/weather.csv"

# Проверка состояния
curl -X GET "http://localhost:8080/api/v1/weather-etl/health"
```

### Мониторинг endpoints

| Endpoint | Description |
|----------|-------------|
| `/actuator/health` | Проверка состояния приложения |
| `/actuator/info` | Информация о приложении |
| `/actuator/metrics` | Метрики приложения |
| `/actuator/prometheus` | Метрики в формате Prometheus |

## ⚙️ Конфигурация

### Основные настройки

```yaml
# application.yml
weather:
  api:
    base-url: https://api.open-meteo.com/v1/forecast
    timeout: 30s
    retry:
      max-attempts: 3
      delay: 1s
  
  default-location:
    latitude: 55.0344    # Новосибирск
    longitude: 82.9434
  
  output:
    csv-path: ./output/weather_data.csv
    batch-size: 1000
```

### Настройки базы данных

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/weather_db
    username: weather_user
    password: weather_pass
    
  jpa:
    hibernate:
      ddl-auto: none
    show-sql: false
    
  flyway:
    enabled: true
    locations: classpath:db/migration
```

### Переменные окружения

```bash
# Настройки базы данных
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/weather_db
SPRING_DATASOURCE_USERNAME=weather_user
SPRING_DATASOURCE_PASSWORD=weather_pass

# Настройки API
WEATHER_API_BASE_URL=https://api.open-meteo.com/v1/forecast
WEATHER_API_TIMEOUT=30s

# Настройки вывода
WEATHER_OUTPUT_CSV_PATH=/app/output/weather_data.csv
WEATHER_OUTPUT_BATCH_SIZE=1000

# Настройки приложения
SPRING_PROFILES_ACTIVE=docker
JAVA_OPTS="-Xms512m -Xmx1024m"
```

## 🐳 Docker

### Сборка образа

```bash
# Сборка Docker образа
docker build -t weather-etl:latest .

# Сборка с тегом версии
docker build -t weather-etl:1.0.0 .
```

### Docker Compose Services

| Service | Port | Description |
|---------|------|-------------|
| `weather-etl` | 8080 | Основное приложение |
| `postgres` | 5432 | База данных PostgreSQL |
| `pgadmin` | 8081 | Веб-интерфейс для управления БД |

### Управление контейнерами

```bash
# Запуск всех сервисов
docker-compose up -d

# Запуск только базы данных
docker-compose up -d postgres

# Остановка всех сервисов
docker-compose down

# Остановка с удалением данных
docker-compose down -v

# Просмотр логов
docker-compose logs -f weather-etl
docker-compose logs -f postgres

# Перезапуск сервиса
docker-compose restart weather-etl

# Масштабирование (если нужно)
docker-compose up -d --scale weather-etl=3
```

### Доступ к сервисам

- **Weather ETL Application**: http://localhost:8080
- **API Health Check**: http://localhost:8080/actuator/health
- **PostgreSQL**: localhost:5432
- **pgAdmin**: http://localhost:8081 (admin@weather-etl.com / admin)

## 👨‍💻 Разработка

### Структура проекта

```
weather-etl-pipeline/
├── src/
│   ├── main/
│   │   ├── java/com/weatheretl/
│   │   │   ├── cli/                 # Command Line Interface
│   │   │   ├── config/              # Конфигурация
│   │   │   ├── controller/          # REST контроллеры
│   │   │   ├── model/               # Модели данных
│   │   │   │   ├── api/             # API модели
│   │   │   │   └── output/          # Выходные модели
│   │   │   ├── repository/          # Репозитории данных
│   │   │   ├── service/             # Бизнес-логика
│   │   │   └── util/                # Утилиты
│   │   └── resources/
│   │       ├── db/migration/        # Flyway миграции
│   │       ├── application.yml      # Основная конфигурация
│   │       └── application-*.yml    # Профили конфигурации
│   └── test/                        # Тесты
├── docker/                          # Docker файлы
├── sql/                            # SQL скрипты
├── output/                         # Выходные CSV файлы
├── docs/                           # Документация
├── build.gradle                    # Gradle конфигурация
├── docker-compose.yml              # Docker Compose
├── Dockerfile                      # Docker образ
└── README.md                       # Этот файл
```

### Настройка среды разработки

1. **IDE конфигурация**
```bash
# IntelliJ IDEA
# Установить плагины: Lombok, Spring Boot

# VS Code
# Установить расширения: 
# - Extension Pack for Java
# - Spring Boot Extension Pack
# - Lombok Annotations Support
```

2. **Lombok настройка**
```bash
# Убедитесь, что Lombok включен в IDE
# IntelliJ IDEA: Settings → Plugins → Lombok
# VS Code: установлен соответствующий extension
```

3. **База данных для разработки**
```bash
# Запуск только PostgreSQL для разработки
docker-compose up -d postgres

# Или использование H2 in-memory базы для тестов
# (настроено в application-test.yml)
```

### Coding Standards

- **Java Code Style**: следуем [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html)
- **Naming Convention**:
    - Classes: PascalCase (`WeatherEtlService`)
    - Methods/Variables: camelCase (`weatherApiClient`)
    - Constants: UPPER_SNAKE_CASE (`MAX_RETRY_ATTEMPTS`)
- **Lombok**: используем аннотации для уменьшения boilerplate кода
- **Documentation**: все public методы должны иметь JavaDoc

## 🧪 Тестирование

### Структура тестов

```
src/test/java/
├── integration/                 # Интеграционные тесты
├── unit/                       # Unit тесты
└── testcontainers/            # Тесты с Testcontainers
```

### Запуск тестов

```bash
# Все тесты
./gradlew test

# Только unit тесты
./gradlew test --tests "*Unit*"

# Только интеграционные тесты
./gradlew test --tests "*Integration*"

# Тесты с покрытием кода
./gradlew test jacocoTestReport

# Просмотр отчета о покрытии
open build/reports/jacoco/test/html/index.html
```

### Примеры тестов

#### Unit тест для конвертера

```java
@Test
void shouldConvertFahrenheitToCelsius() {
    // Given
    Double fahrenheit = 68.0;
    
    // When
    Double celsius = WeatherConverter.fahrenheitToCelsius(fahrenheit);
    
    // Then
    assertThat(celsius).isEqualTo(20.0);
}
```

#### Интеграционный тест с Testcontainers

```java
@SpringBootTest
@Testcontainers
class WeatherEtlIntegrationTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15")
            .withDatabaseName("weather_test")
            .withUsername("test")
            .withPassword("test");
    
    @Test
    void shouldProcessWeatherDataSuccessfully() {
        // Тест полного ETL процесса
    }
}
```

### Тестирование API

```bash
# Использование curl для тестирования API
curl -X GET "http://localhost:8080/actuator/health"

# Использование httpie (если установлен)
http GET localhost:8080/api/v1/weather-etl/stats

# Использование Postman
# Импортируйте коллекцию из docs/postman/
```

## 📊 Мониторинг

### Метрики приложения

Приложение предоставляет метрики через Spring Boot Actuator:

```bash
# Основные метрики
curl http://localhost:8080/actuator/metrics

# Метрики JVM
curl http://localhost:8080/actuator/metrics/jvm.memory.used

# Метрики HTTP запросов
curl http://localhost:8080/actuator/metrics/http.server.requests

# Метрики базы данных
curl http://localhost:8080/actuator/metrics/hikaricp.connections.active
```

### Health Checks

```bash
# Общее состояние приложения
curl http://localhost:8080/actuator/health

# Детальная информация о состоянии
curl http://localhost:8080/actuator/health | jq

# Состояние базы данных
curl http://localhost:8080/actuator/health/db
```

### Логирование

Логи приложения доступны в нескольких форматах:

```bash
# Просмотр логов в Docker
docker-compose logs -f weather-etl

# Логи сохраняются в файл (в Docker)
docker exec weather-etl-app cat /app/logs/weather-etl.log

# Локальная разработка
tail -f logs/spring.log
```

### Prometheus метрики

```bash
# Метрики в формате Prometheus
curl http://localhost:8080/actuator/prometheus

# Интеграция с Prometheus/Grafana
# См. docker-compose-monitoring.yml (если доступен)
```

## 🔧 Troubleshooting

### Частые проблемы

1. **Проблемы с подключением к базе данных**
```bash
# Проверить статус PostgreSQL
docker-compose ps postgres

# Проверить логи базы данных
docker-compose logs postgres

# Проверить подключение
docker exec -it weather-etl-postgres psql -U weather_user -d weather_db -c "SELECT 1;"
```

2. **Проблемы с API Open-Meteo**
```bash
# Проверить доступность API
curl "https://api.open-meteo.com/v1/forecast?latitude=55.0344&longitude=82.9434&hourly=temperature_2m"

# Проверить логи приложения
docker-compose logs weather-etl | grep -i "api"
```

3. **Проблемы с CSV экспортом**
```bash
# Проверить права на запись
ls -la output/

# Создать директорию для вывода
mkdir -p output && chmod 755 output
```

4. **Out of Memory ошибки**
```bash
# Увеличить память для JVM
export JAVA_OPTS="-Xms1g -Xmx2g"

# Или в docker-compose.yml
environment:
  JAVA_OPTS: "-Xms1g -Xmx2g"
```

### Диагностика

```bash
# Проверка состояния всех сервисов
docker-compose ps

# Проверка использования ресурсов
docker stats

# Проверка сетевого подключения
docker network inspect weather-network

# Проверка томов данных
docker volume ls
docker volume inspect weather-etl-pipeline_postgres_data
```

## 📋 Roadmap

### Планируемые улучшения

- [ ] **Веб-интерфейс**: React frontend для управления ETL процессами
- [ ] **Планировщик задач**: Cron-like планирование ETL задач
- [ ] **Уведомления**: Email/Slack уведомления о статусе выполнения
- [ ] **Metrics Dashboard**: Grafana дашборды для мониторинга
- [ ] **Кэширование**: Redis для кэширования API ответов
- [ ] **Многопоточность**: Параллельная обработка больших объемов данных
- [ ] **API версионирование**: Поддержка нескольких версий API
- [ ] **Аутентификация**: JWT токены для безопасности API

### Технические улучшения

- [ ] **Тестирование**: Увеличение покрытия тестами до 90%+
- [ ] **Performance**: Оптимизация производительности для больших датасетов
- [ ] **Resilience**: Улучшение обработки ошибок и восстановления
- [ ] **Observability**: Distributed tracing с Zipkin/Jaeger
- [ ] **Security**: HTTPS, аутентификация, авторизация

### Процесс разработки

1. Fork проекта
2. Создайте feature branch (`git checkout -b feature/amazing-feature`)
3. Commit изменения (`git commit -m 'Add amazing feature'`)
4. Push в branch (`git push origin feature/amazing-feature`)
5. Создайте Pull Request

### Code Review

- Все изменения должны проходить code review
- Убедитесь, что все тесты проходят
- Следуйте установленным coding standards
- Добавьте тесты для новой функциональности

## 📄 License

Этот проект лицензируется под MIT License - см. файл [LICENSE](LICENSE) для деталей.

## 📞 Поддержка

- **Issues**: [GitHub Issues](https://github.com/asachiyigor/WeatherEtlApplication/issues)
- **Discussions**: [GitHub Discussions](https://github.com/asachiyigor/WeatherEtlApplication/discussions)
- **Email**: asachiyigor@gmail.com

## 🙏 Благодарности

- [Open-Meteo](https://open-meteo.com/) за предоставление бесплатного API погодных данных
- [Spring Boot](https://spring.io/projects/spring-boot) команда за отличный фреймворк
- [PostgreSQL](https://www.postgresql.org/) сообщество за надежную базу данных

---

**Сделано с ❤️ для обработки погодных данных**