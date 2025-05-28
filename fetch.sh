# This is set to Dec 1 2024 00:00 - 26 May 2025 23:59
# https://amplitude.com/docs/apis/analytics/export
curl --location --request GET 'https://amplitude.com/api/2/export?start=20241201T00&end=20250526T23' \
-u "$AMPLITUDE_PROJECT_API_KEY:$AMPLITUDE_PROJECT_SECRET_KEY" --output amplitude-export.zip
