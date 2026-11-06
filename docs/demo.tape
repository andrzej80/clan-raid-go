# Запись демо для README: docs/demo.gif
#
#   vhs docs/demo.tape        (нужны vhs, ttyd и ffmpeg; запускать из корня репозитория)
#
# Подготовка (сборка образов, инфраструктура, миграции) снимается скрытно и в GIF не попадает.
# Потом botsim бьёт по боссу в фоне, а raidwatch показывает его до гибели и итоговый экран.
# Параметры нагрузки: HP, PLAYERS, RATE (см. loadtest/demo.sh).
Output docs/demo.gif

Require bash

Set Shell "bash"
Set FontSize 15
Set Width 1100
Set Height 640
Set Padding 16
Set Theme "Dracula"
Set TypingSpeed 40ms

Hide
# Вывод сборки скрыт: терминал vhs медленно рисует поток логов, и подготовка затягивается.
# Маркер READY-2 вычисляется оболочкой, поэтому не совпадает с набранной командой.
# Wait по умолчанию смотрит только последнюю строку, поэтому Wait+Screen.
Type "loadtest/demo.sh prepare >/dev/null 2>&1; echo READY-$((1+1))"
Enter
Wait+Screen@15m /READY-2/
Type "clear"
Enter
Show

Type "loadtest/demo.sh run"
Sleep 500ms
Enter

# Босс умирает за ~30 с; запас на подключение и итоговый экран.
Wait+Screen@120s /Босс повержен/
Sleep 6s

Type "q"
Sleep 1s
