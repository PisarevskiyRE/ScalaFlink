

import java.time.{Duration, LocalTime, ZoneId, ZonedDateTime}




// Пример длительности в миллисекундах
val d: Long = 1632122400000L // Пример даты и времени в миллисекундах

// Преобразование длительности в `ZonedDateTime` (с зоной UTC)
val dateTime: ZonedDateTime = ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(d), ZoneId.of("UTC"))

// Извлечение времени из `ZonedDateTime`
val localTime: LocalTime = dateTime.toLocalTime

// Создание `Duration` на основе времени
val duration: Duration = Duration.ofHours(localTime.getHour).plusMinutes(localTime.getMinute).plusSeconds(localTime.getSecond)

// Вывод результатов
println(s"Длительность в миллисекундах: $d")
println(s"Время: $localTime")
println(s"Преобразованное время в Duration: $duration")


