# Create Index Mapping 
PUT /weather
{
  "mappings": {
    "properties": {
      "date": { "type": "date" },
      "temperature_2m": { "type": "float" },
      "relative_humidity_2m": { "type": "float" },
      "rain": { "type": "float" },
      "snowfall": { "type": "float" },
      "weather_code": { "type": "integer" },
      "surface_pressure": { "type": "float" },
      "cloud_cover": { "type": "float" },
      "cloud_cover_low": { "type": "float" },
      "cloud_cover_high": { "type": "float" },
      "wind_direction_10m": { "type": "integer" },
      "wind_direction_100m": { "type": "integer" },
      "soil_temperature_28_to_100cm": { "type": "float" }
    }
  }
}

# Verify the Data in Elasticsearch
GET /weather/_search
{
  "query": {
    "match_all": {}
  }
}

#Search by specific field
GET /weather/_search
{
  "query": {
    "range": {
      "temperature_2m": {
        "gte": 20,
        "lte": 30
      }
    }
  }
}

#View index mapping
GET /weather/_mapping

#Monitor Indexing Status
GET /weather/_count

# Delete the Index (if required)
DELETE /weather

#Average Temperature
GET /weather/_search
{
  "size": 0, 
  "aggs": {
    "avg_temperature": {
      "avg": {
        "field": "temperature_2m"
      }
    }
  }
}

#Maximum and Minimum Humidity
GET /weather/_search
{
  "size": 0, 
  "aggs": {
    "max_humidity": {
      "max": {
        "field": "relative_humidity_2m"
      }
    },
    "min_humidity": {
      "min": {
        "field": "relative_humidity_2m"
      }
    }
  }
}

#Histogram: Temperature Distribution
GET /weather/_search
{
  "size": 0,
  "aggs": {
    "temperature_distribution": {
      "histogram": {
        "field": "temperature_2m",
        "interval": 5
      }
    }
  }
}

#Date Histogram: Temperature over Time
GET /weather/_search
{
  "size": 0,
  "aggs": {
    "temperature_over_time": {
      "date_histogram": {
        "field": "date",
        "calendar_interval": "day"
      },
      "aggs": {
        "avg_temperature": {
          "avg": {
            "field": "temperature_2m"
          }
        }
      }
    }
  }
}

#Filter Data by Conditions:
#You can filter your data to focus on specific conditions, such as retrieving records where rain is more than 0 (indicating rainy days)
GET /weather/_search
{
  "query": {
    "range": {
      "rain": {
        "gt": 0
      }
    }
  }
}

#Terms Aggregation: Group by Weather Code
GET /weather/_search
{
  "size": 0,
  "aggs": {
    "weather_conditions": {
      "terms": {
        "field": "weather_code"
      }
    }
  }
}

#Filter by Date Range
GET /weather/_search
{
  "query": {
    "range": {
      "date": {
        "gte": "now-30d/d",
        "lte": "now/d"
      }
    }
  }
}

#Average Temperature by Weather Code
GET /weather/_search
{
  "size": 0,
  "aggs": {
    "weather_conditions": {
      "terms": {
        "field": "weather_code"
      },
      "aggs": {
        "avg_temperature": {
          "avg": {
            "field": "temperature_2m"
          }
        }
      }
    }
  }
}

#Top N Results: Highest Rainfall Days
GET /weather/_search
{
  "size": 5,
  "sort": [
    {
      "rain": {
        "order": "desc"
      }
    }
  ]
}

