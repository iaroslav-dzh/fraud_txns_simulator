# Конвертация ноутбуков в html

```
jupyter nbconvert --output-dir='./docs' --to html notebooks_clean/*.ipynb
```

# Стили для отображения на github pages

## Чтобы был скролл для таблиц и пэддинг для ноутбука

**В чате искать**
```
Отлично — сделаем так, чтобы твой Jupyter HTML выглядел прилично на GitHub Pages, с горизонтальным скроллом для широких таблиц и вывода.
```

**Вставлять перед `</head>`**

```html
<style>
/* Горизонтальный скролл для широких таблиц */
table, .output_subarea {
    display: block;
    overflow-x: auto;
    white-space: nowrap;
}

/* Не даём выводам выходить за экран */
.output_area {
    overflow-x: auto;
}

/* Немного отступов и скруглений (по желанию) */
.output_subarea {
    padding: 8px;
    border-radius: 6px;
    background-color: #f8f8f8;
}
</style>
<style>
/* Центрирование и ограничение ширины */
body {
    max-width: 960px;
    margin: 0 auto;
    padding: 1.5em;
    background-color: #ffffff;
    font-family: sans-serif;
}

/* Отступы вокруг блоков вывода */
.output_area {
    margin-bottom: 1em;
    overflow-x: auto;
}

/* Убедимся, что код и таблицы не вылезают */
table, .output_subarea {
    display: block;
    overflow-x: auto;
    white-space: nowrap;
}

/* Код и вывод — светло-серый фон, скругления */
.output_subarea {
    background-color: #f8f8f8;
    padding: 8px;
    border-radius: 6px;
}

/* (опционально) Немного сужаем Markdown-ячейки */
div.text_cell_render {
    line-height: 1.6;
    font-size: 16px;
}
</style>
```


### Полный скрипт вставки этих стилей во все html файлы с `</head>` в указанной папке либо текущей директории

В [чате](https://chatgpt.com/c/6880984b-5d28-832b-90ba-bc1fdb65673f) искать. В репе это скрипт `inject_minimal_styles.py`
```
давай скрипт автоматической вставки минимальных стилей для всех html в указанной папке
```