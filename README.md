| Название проекта    | Описание | Стек       |
|----------|---------|-------------|
| [A/B тесты](https://github.com/SergeyTsedrik/simDA_karpov.courses/tree/main/AB-test)| Проект направлен на оценку эффективности нового алгоритма рекомендации постов, применяемого во 2-й группе пользователей, с целью проверки гипотезы о его способности увеличить CTR по сравнению с контрольной группой 1 в ходе A/B-теста      | Pandas, Pandahouse, SciPy (scipy.stats), Seaborn, Matplotlib и NumPy|
|[Рассчет размера выборки](https://github.com/SergeyTsedrik/simDA_karpov.courses/tree/main/Sample_size)| Проект направлен на исследование влияния нового алгоритма рекомендаций на количество лайков, получаемых пользователями. Для проверки этой гипотезы планируется провести симуляцию Монте-Карло, что позволит оценить потенциальное влияние нового алгоритма на пользовательскую активность и определить его эффективность в повышении количества лайков.| Pandahouse, pandas, tqdm (для отображения прогресса), seaborn и matplotlib, numpy и scipy.stats.|
|[Прогнозирование метрик](https://github.com/SergeyTsedrik/simDA_karpov.courses/tree/main/predicting_metrics)|Проект направлен на поддержку и стимулирование пользовательской активности в нашем продукте через организацию флэшмоба в ленте новостей. Задача заключается в оценке эффективности флэшмоба, анализируя метрики вовлеченности, охвата и общего числа участников, а также сравнивая их с показателями до и после проведения акции.| orbit (библиотека для байесовского прогнозирования временных рядов), pandas, pandahouse, numpy, seaborn, matplotlib, arviz (библиотека для визуализации результатов).  |
|[ETL проект для анализа пользователей](https://github.com/SergeyTsedrik/simDA_karpov.courses/tree/main/building_ETL)| Проект заключается в разработке ETL-процесса для анализа пользовательского поведения на платформе, который будет выполняться ежедневно и обновлять метрики в ClickHouse. Используя Apache Airflow, мы создадим Directed Acyclic Graph (DAG) для обработки данных из двух таблиц| Pandas, StringIO, Pandahouse, Requests, Apache Airflow (платформа для автоматизации рабочих процессов, использующая DAG для управления задачами|
|[Автоматизация отчетности в Telegram](https://github.com/SergeyTsedrik/simDA_karpov.courses/tree/main/automation_reporting) | Проект заключается в автоматизации процесса отправки ежедневной аналитической сводки по ключевым метрикам нашего приложения в Telegram, используя бота и Apache Airflow для планирования задач.| Telegram API, NumPy, Matplotlib, Seaborn, Pandas, pandahouse, ClickHouse, Apache Airflow для автоматизации процессов.|
|[Поиск аномалий](https://github.com/SergeyTsedrik/simDA_karpov.courses/tree/main/anomaly_search)|Проект заключается в создании системы алертов для мониторинга ключевых метрик приложения с целью своевременного обнаружения аномалий в пользовательском поведении.|Telegram API, numpy, pandas, matplotlib, seaborn, Airflow.
| [Построение дашборда](https://github.com/SergeyTsedrik/simDA_karpov.courses/tree/main/building_dashboard) | В рамках задания сосредоточился на создании дашборда, который анализирует взаимодействие пользователей с двумя ключевыми сервисами приложения: Лентой новостей и Сервисом сообщений. После успешного создания дашборда для Ленты новостей, мы расширили наш анализ, чтобы лучше понять, как пользователи взаимодействуют с обоими сервисами. | SQL, Superset |
