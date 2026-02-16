import logging
import os

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from card_bot import CardBot


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

card_service = CardBot(log_path="card_changes.log")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command."""
    if update.message is None:
        return

    await update.message.reply_text(
        "Привет! Я бот для подготовки карточек.\n"
        "Отправьте строку в формате:\n"
        "ФИО; пол; организация RU; организация EN; должность RU; должность EN"
    )


async def process_card_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Create and return a normalized card from user text."""
    if update.message is None or update.message.text is None:
        return

    text = update.message.text.strip()
    if not text:
        await update.message.reply_text("Пустое сообщение. Пришлите данные для карточки.")
        return

    user = update.effective_user
    if user is not None:
        created_by = user.username or user.full_name
    else:
        created_by = "telegram"

    try:
        card = card_service.create_card(text, created_by=created_by)
        await update.message.reply_text(card_service.render_card(card))
    except Exception:
        logger.exception("Ошибка обработки входного сообщения")
        await update.message.reply_text(
            "Не удалось обработать сообщение. Проверьте формат:\n"
            "ФИО; пол; организация RU; организация EN; должность RU; должность EN"
        )


def get_token() -> str:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError(
            "Переменная окружения TELEGRAM_BOT_TOKEN не установлена. "
            "Добавьте токен и запустите снова."
        )
    return token


def main() -> None:
    token = get_token()

    application = Application.builder().token(token).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, process_card_request))

    logger.info("Бот карточек запущен и ожидает сообщения...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
