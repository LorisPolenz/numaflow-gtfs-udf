# Database Format

The database needs to store tables for two feed versions. The current one and the one before. This way it is ensured that there is never the case that the required feedversion is not (yet) available. 

| FeedVersion  | Source  | TableName |
|---|---|---|
| 20251220  | stop_times  | 20251220_stop_times |
| 20251217  |  stop_times |  20251217_stop_times |

> The housekeeping service is responsible to update the feeds.db file.

> The side input is responsible to provide the last two feed versions.