---
title: "PHORECAST: An Open Source Framework for Photovoltaic Power Forecasting Using Machine Learning"
tags:
  - Python
  - photovoltaic
  - machine learning
  - energy forecasting
  - renewable energy
authors:
  - name: Felix Theusch
    affiliation: 1
  - name: Paul Heisterkamp
    affiliation: 1
  - name: Joscha Grüger
    affiliation: "1, 2"
  - name: Sascha Stülb
    affiliation: 1
  - name: Maximilian Hoffmann
    affiliation: 1
  - name: Ralph Bergmann
    affiliation: "1, 2"
affiliations:
  - name: German Research Center for Artificial Intelligence (DFKI), Branch Trier University, 54296 Trier, Germany
    index: 1
  - name: Artificial Intelligence and Intelligent Information Systems, Trier University, 54296 Trier, Germany
    index: 2
date: 28 June 2026
bibliography: references.bib
---

# Summary

Accurate forecasting of photovoltaic (PV) power generation is important for grid operation, energy management, and the integration of renewable electricity into modern energy systems. **PHORECAST** is an open-source software framework for PV power forecasting with machine learning. It combines PV and weather data handling, configurable forecasting workflows, persistent storage, and web/API access in a deployment-oriented framework.

The submitted JOSS software artifact is the `phorecast` framework. It provides data ingestion and loader configuration, workflow orchestration, storage, Docker-based deployment, documentation, and the JOSS paper. The reusable forecasting functionality is separated into the independently installable `phorecast-ml` package, which provides preprocessing, dataset generation, metrics/losses, and LSTM-based forecasting functionality used by the framework. This split allows researchers to use the machine-learning core without the database and deployment stack, while retaining a full framework path for deployment-oriented PV forecasting workflows.

# Statement of need

Photovoltaic generation is variable because it depends on meteorological conditions, daylight, site configuration, and the technical condition of the installation [@Iheanetu_2022]. This variability affects grid operators, energy suppliers, traders, prosumers, and operators of energy-intensive infrastructure. Forecasts help these actors anticipate fluctuations, schedule flexible demand, reduce balancing risk, and improve self-consumption and battery utilization [@Ahmed_2020; @Luthander_2015].

PV forecasting approaches include statistical time-series methods, physical models, machine-learning methods, and hybrid or ensemble approaches [@Sobri_2018; @Gupta_2021]. In practical research workflows, however, model development is only one part of the problem. Researchers also need repeatable handling of PV measurements and weather forecasts, consistent preprocessing and dataset construction, configurable model execution, storage of forecasts and artifacts, and a route from local experiments to deployment-oriented operation.

PHORECAST addresses this integration layer. It is intended for installation-level or inverter-level PV forecasting workflows in which PV measurements and weather forecasts are combined to train and run machine-learning models. Its contribution is not a broad collection of forecasting algorithms, but a PV-specific framework that connects data ingestion, workflow orchestration, reusable machine-learning functionality, and application infrastructure in a reproducible open-source system.

# State of the field

Existing open-source software covers several important parts of the PV and forecasting workflow. `pvlib python` provides a widely used foundation for PV system performance modelling and physical PV calculations [@Anderson2023], while `pvOps` supports empirical analysis of heterogeneous PV field-operation data [@Bonney2023]. The Solar Forecast Arbiter addresses a complementary part of the workflow by providing infrastructure for repeatable evaluation of solar, irradiance, and net-load forecasts [@Hansen2019SolarForecastArbiter]. These tools are important reference points for PV research software, but they do not primarily provide a configurable machine-learning framework that combines PV/weather data handling, model execution, storage, and deployment-oriented operation.

Broader energy and time-series software addresses related problems at different levels of abstraction. OpenSTEF provides open tooling for short-term energy forecasting pipelines [@OpenSTEF], while general-purpose libraries such as Darts provide broad forecasting model APIs [@Herzen2022Darts]. PHORECAST is not intended to replace these domain-general forecasting libraries. Instead, it focuses on the PV forecasting workflow around the model: data loaders, orchestration, persistence, web/API access, Docker deployment, and reusable PV-focused preprocessing and LSTM forecasting functionality. The separation between `phorecast` and `phorecast-ml` is central to this contribution because it lets the same forecasting core support lightweight research use and full framework deployments.

# Software design

PHORECAST uses a two-layer architecture. The `phorecast` repository is the full framework layer and remains the primary JOSS artifact. It contains the configurable component system for target and weather loaders, the event engine for workflow orchestration, InfluxDB and SQLite integration for time-series data and configuration metadata, the Flask web interface and HTTP API, Docker Compose deployment files, documentation, and the paper. In the documented full-framework workflow, users configure PV measurement loaders, weather loaders, and forecasting models; the framework executes these components and stores generated forecasts for inspection through the application stack.

The `phorecast-ml` package is the reusable machine-learning core. It can be installed independently and imported without the database, web interface, or Docker deployment. Its current documented scope includes preprocessing utilities, solar-position features, time-window and dataset generation, train/test splitting, custom metrics/losses, and LSTM-based training and inference using TensorFlow/Keras.

![Overview of the PHORECAST architecture. The phorecast framework handles data ingestion, workflow orchestration, storage, web/API access, and Docker-based deployment, while phorecast-ml provides the reusable preprocessing and model-training core.](Forecast_architecture.png)

The design trade-off is deliberate. Keeping orchestration, storage, web/API access, and deployment in `phorecast` supports deployment-oriented PV forecasting workflows and reviewer-friendly Docker usage. Separating preprocessing and LSTM forecasting into `phorecast-ml` keeps the forecasting core reusable for lightweight experiments and directly addresses the need for installation, import, and testing without database infrastructure. This structure narrows the package boundaries while preserving a complete framework path for PV forecasting research.

# Research impact statement

PHORECAST has been used in DFKI and Trier University research contexts as a reusable baseline for PV forecasting workflows. It supports benchmarking-style experiments in which researchers compare the effect of different weather models while keeping the forecasting baseline model and workflow fixed. It is also being applied to cloud/edge continuum investigations, especially settings that combine global model training with local inference, and to the development and evaluation of multi-model ensemble approaches for PV forecasting.

The framework is used in project contexts that connect PV forecasting to applied energy-management questions. In the context of DZW [@DFKI_DZW], PHORECAST supports research workflows on integrating PV power generation into drinking-water distribution and wastewater treatment processes. In the context of HIP-EMIL [@DFKI_HIPEMIL], it supports research workflows in predictive energy management. In the context of SOWEKI [@DFKI_SOWEKI], it supports research workflows on sector coupling between PV generation and energy demands in drinking-water infrastructure.

These uses are project-specific research applications rather than evidence of broad community adoption. Their significance for JOSS is that PHORECAST provides a reusable and reproducible software layer for PV forecasting experiments, metric-based comparison of generated forecasts, ensemble development, and deployment-oriented studies where the same workflow must connect research code with operational data and application infrastructure.

# AI usage disclosure

OpenAI ChatGPT and Codex were used during the revision of this work for auxiliary tasks, including repository review, documentation drafting and restructuring, manuscript editing, figure planning, proofreading for clarity and consistency, and support with small code or repository maintenance tasks where applicable. AI-assisted suggestions were reviewed, edited, tested where relevant, and validated by the authors before inclusion. All scientific ideas, software design decisions, architectural choices, project-specific claims, references, analyses, interpretations, and final manuscript wording were reviewed and approved by the authors, who retain full responsibility for the submitted software and paper.

# Acknowledgements

The authors thank the [Smart Resource Management research group at Trier University](https://github.com/smart-resource-management-trier) for feedback during development and acknowledge funding from the Ministry for Climate Protection, Environment, Energy and Mobility of Rhineland-Palatinate (MKUEM).
