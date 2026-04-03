#!/bin/bash

# 1. Получаем текущую версию (если тегов нет, начнем с v0.0.1)
CURRENT_VERSION=$(git describe --tags --abbrev=0 2>/dev/null)

if [ -z "$CURRENT_VERSION" ]; then
    CURRENT_VERSION="v0.0.1"
    echo "Теги не найдены. Начинаем с $CURRENT_VERSION"
else
    echo "Текущая версия: $CURRENT_VERSION"
fi

# Убираем префикс 'v', если он есть, чтобы работать только с цифрами
VERSION_WITHOUT_V="${CURRENT_VERSION#v}"

# Разбиваем версию на мажорную, минорную и патч (например: 0 0 9)
IFS='.' read -r MAJOR MINOR PATCH <<< "$VERSION_WITHOUT_V"

# 2. Инкрементируем патч-версию
PATCH=$((PATCH + 1))

# Логика переноса (если патч стал 10, сбрасываем его в 0 и увеличиваем минорную)
if [ "$PATCH" -ge 10 ]; then
    PATCH=0
    MINOR=$((MINOR + 1))
fi

# Собираем новую версию обратно
NEW_VERSION="v${MAJOR}.${MINOR}.${PATCH}"
echo "Новая версия: $NEW_VERSION"

# 3. Выполнение Git команд
echo "Добавляем файлы..."
git add .

# Проверяем, есть ли вообще изменения для коммита
if git diff --staged --quiet; then
    echo "Нет изменений для коммита, создаем пустой коммит для тега."
    git commit --allow-empty -m "Release $NEW_VERSION"
else
    git commit -m "Release $NEW_VERSION"
fi

echo "Создаем тег $NEW_VERSION..."
git tag "$NEW_VERSION"

echo "Отправляем ветку main в origin..."
git push origin main

echo "Отправляем тег в origin..."
git push origin "$NEW_VERSION"

echo "Готово! Версия $NEW_VERSION успешно опубликована."