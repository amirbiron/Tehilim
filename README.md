# Tehillim Bot — Traditional Splits (Hebrew)

בוט תהילים לטלגרם עם סימנייה אישית, קריאה לפי הסדר, וחלוקה שבועית + ל' בחודש לפי המנהג.
מוכן ל-Render (Worker) עם דיסק מתמיד (SQLite).

פקודות: /start, /next, /prev, /where, /goto, /daily, /weekly, /load_texts

## פקודת אדמין: טעינת טקסטים אוטומטית
- הגדר `ADMIN_USER_ID` למזהה שלך בטלגרם.
- מתוך שיחה עם הבוט:
```
/load_texts
```
הבוט יוריד את כל ספר תהילים (1–150) מ-Sefaria וישמור ל-`data/tehillim.json`. אם חסר `psalm119_parts.json` — יווצר טמפלט לעריכה.
