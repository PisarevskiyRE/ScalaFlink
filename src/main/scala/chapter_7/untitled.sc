import java.time.Duration

val timeString = "0:00:01"
val parts = timeString.split(":").map(_.toLong)

val duration = Duration.ofHours(parts(0)).plusMinutes(parts(1)).plusSeconds(parts(2))