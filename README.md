# 🛡 Hero Scanner PRO · Ultra Premium
Bu loyiha oddiy skaner emas, balki talabalar uchun to'liq avtomatlashtirilgan "Shaxsiy Yordamchi" (Assistant) hisoblanadi.

## 🚀 Asosiy Imkoniyatlar (Features)
1. **Smart Scanner (Aqlli Skaner):** Skaner jarayonida token eskirgan bo'lsa, foydalanuvchidan parol so'ramaydi, orqa fonda o'zi qayta login qilib, tokenni yangilab skanerni urib yuboradi.
2. **Shadow Mode (Kuzatish rejimi):** Admin ma'lum bir foydalanuvchini "Kuzatish"ga qo'ysa, o'sha foydalanuvchi QR kodni skaner qilganda, Adminning barcha akkauntlari ham orqa fonda qo'shilib darsga kirib ketadi.
3. **Auto-Schedule Sync:** Yangi Hero akkaunt qo'shilgan zahoti, tizim nafaqat tokenni oladi, balki ushbu talabaning butun semestr dars jadvalini tortib olib bazaga saqlaydi.
4. **Haptic & Audio UI:** Telegram Mini App ichida tugmalar bosilganda telefon tebranadi (Vibratsiya) va maxsus ovozlar chiqadi. Orqa fonda uzluksiz ishlaydigan Awwwards darajasidagi animatsiya.
5. **Google Calendar Integratsiyasi:** Premium foydalanuvchilar o'zlarining shaxsiy kalendarlarini botga ulab, Hero darslari bilan ustma-ust tushish (Overlap) holatlarini aniqlashlari mumkin.
6. **Auto-Login:** Foydalanuvchi bir marta login qilsa, keyingi safar to'g'ridan-to'g'ri o'z paneliga tushadi.

## 🗄 Ma'lumotlar Bazasi Tuzilishi
- `app_users`: Foydalanuvchi asosiy ma'lumotlari (Telegram ID, muddat/deadline, shadow_targets, google_tokens).
- `hero_accounts`: Hero loginalar va jonli tokenlar.
- `user_schedule_slots`: Foydalanuvchining tortib olingan dars jadvallari.
- `scan_logs`: Kim, qachon, nechta urgani haqida to'liq tarix.
- `archived_accounts`: O'chirilgan akkauntlar xavfsizlik uchun saqlanadigan arxiv.
