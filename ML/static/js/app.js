/**
 * Crypto ML — Report UI + Chart.js (cdnjs) + FastAPI
 */

const CHART_COLORS = {
    grid: 'rgba(15, 23, 42, 0.06)',
    text: '#64748b',
    primary: '#0078d4',
    primaryFill: 'rgba(0, 120, 212, 0.12)',
    secondary: '#ca5010',
    success: '#107c10',
    danger: '#a4262c',
    violet: '#5c2d91',
    grid: 'rgba(0, 0, 0, 0.06)',
    text: '#605e5c',
};

Chart.defaults.color = '#605e5c';
Chart.defaults.borderColor = 'rgba(0, 0, 0, 0.06)';
Chart.defaults.font.family = "'Segoe UI', 'Inter', system-ui, sans-serif";
Chart.defaults.font.size = 11;

let featureChart = null;
let lstmChart = null;
let lstmLossChart = null;
let anomalyChart = null;
let priceChart = null;

function fmt(n, decimals = 2) {
    if (n === null || n === undefined || (typeof n === 'number' && isNaN(n))) return '--';
    return Number(n).toLocaleString('en-US', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals,
    });
}

function fmtPrice(n) {
    if (n === null || n === undefined) return '--';
    return '$' + fmt(n, 2);
}

function setModelHints(data) {
    const banner = document.getElementById('modelHint');
    const lstmHint = document.getElementById('lstmChartHint');
    const needTrain = data && (data.lstm_ready === false || data.training_metrics_ready === false);
    if (banner) {
        banner.hidden = !needTrain;
    }
    if (lstmHint) {
        lstmHint.hidden = !(data && data.lstm_ready === false);
    }
}

async function fetchPredictions() {
    try {
        const res = await fetch('/api/predictions');
        const data = await res.json();
        updatePredictionUI(data);
        setModelHints(data);
    } catch (e) {
        console.warn('fetchPredictions error:', e);
    }
}

function updatePredictionUI(data) {
    const price = data.current_price;
    document.getElementById('tickerPrice').textContent = fmtPrice(price);

    const changeEl = document.getElementById('tickerChange');
    if (data.lstm_change_pct !== null && data.lstm_change_pct !== undefined) {
        const pct = data.lstm_change_pct;
        changeEl.textContent = (pct >= 0 ? '+' : '') + fmt(pct, 2) + '%';
        changeEl.className = 'ticker-change ' + (pct >= 0 ? 'up' : 'down');
    } else {
        changeEl.textContent = '--';
        changeEl.className = 'ticker-change';
    }

    const t = new Date().toLocaleTimeString('vi-VN');
    document.getElementById('lastUpdate').textContent = 'Cập nhật: ' + t;
    const fp = document.getElementById('filterPaneUpdate');
    if (fp) fp.textContent = t;

    const xgbSignal = document.getElementById('xgbSignal');
    xgbSignal.textContent = data.xgboost_signal || '--';
    xgbSignal.className = 'signal-value ' + (data.xgboost_signal === 'BULLISH' ? 'bull' : 'bear');
    document.getElementById('xgbProb').textContent =
        'Probability: ' +
        (data.xgboost_bull_prob !== null ? fmt(data.xgboost_bull_prob * 100, 1) + '%' : '--');
    const xgbBox = document.getElementById('xgbSignalBox');
    xgbBox.className =
        'signal-box ' + (data.xgboost_signal === 'BULLISH' ? 'bullish' : 'bearish');

    const lgbmSignal = document.getElementById('lgbmSignal');
    lgbmSignal.textContent = data.lgbm_signal || '--';
    lgbmSignal.className = 'signal-value ' + (data.lgbm_signal === 'BULLISH' ? 'bull' : 'bear');
    document.getElementById('lgbmProb').textContent =
        'Probability: ' +
        (data.lgbm_bull_prob !== null ? fmt(data.lgbm_bull_prob * 100, 1) + '%' : '--');
    const lgbmBox = document.getElementById('lgbmSignalBox');
    lgbmBox.className =
        'signal-box ' + (data.lgbm_signal === 'BULLISH' ? 'bullish' : 'bearish');

    document.getElementById('lstmCurrent').textContent = fmtPrice(price);
    document.getElementById('lstmPredicted').textContent = fmtPrice(data.lstm_predicted_price);
    const changePct = document.getElementById('lstmChangePct');
    if (data.lstm_change_pct !== null && data.lstm_change_pct !== undefined) {
        const pct = data.lstm_change_pct;
        changePct.textContent = (pct >= 0 ? '▲ +' : '▼ ') + fmt(pct, 4) + '%';
        changePct.className = 'stat-sub ' + (pct >= 0 ? 'up' : 'down');
    } else {
        changePct.textContent = '';
        changePct.className = 'stat-sub';
    }

    const predBox = document.getElementById('lstmPredBox');
    if (data.lstm_predicted_price && price) {
        predBox.style.borderColor =
            data.lstm_predicted_price > price
                ? 'rgba(5, 150, 105, 0.45)'
                : 'rgba(220, 38, 38, 0.35)';
    }

    const statusEl = document.getElementById('anomalyStatus');
    const statusBox = document.getElementById('anomalyStatusBox');
    if (data.is_anomaly) {
        statusEl.textContent = 'ANOMALY';
        statusEl.style.color = 'var(--danger)';
        statusBox.className = 'stat-box anomaly';
    } else {
        statusEl.textContent = 'NORMAL';
        statusEl.style.color = 'var(--success)';
        statusBox.className = 'stat-box normal';
    }
    document.getElementById('anomalyScore').textContent =
        data.anomaly_score !== null && data.anomaly_score !== undefined
            ? fmt(data.anomaly_score, 4)
            : '--';
}

async function fetchTrainingResults() {
    try {
        const res = await fetch('/api/training-results');
        const data = await res.json();

        if (data.xgboost) {
            document.getElementById('xgbAcc').textContent = fmt(data.xgboost.accuracy * 100, 1) + '%';
            document.getElementById('xgbPrec').textContent =
                fmt(data.xgboost.precision * 100, 1) + '%';
            document.getElementById('xgbRec').textContent = fmt(data.xgboost.recall * 100, 1) + '%';
            document.getElementById('xgbF1').textContent = fmt(data.xgboost.f1 * 100, 1) + '%';
        }
        if (data.lightgbm) {
            document.getElementById('lgbmAcc').textContent =
                fmt(data.lightgbm.accuracy * 100, 1) + '%';
            document.getElementById('lgbmPrec').textContent =
                fmt(data.lightgbm.precision * 100, 1) + '%';
            document.getElementById('lgbmRec').textContent =
                fmt(data.lightgbm.recall * 100, 1) + '%';
            document.getElementById('lgbmF1').textContent = fmt(data.lightgbm.f1 * 100, 1) + '%';
        }
        if (data.lstm) {
            document.getElementById('lstmMSE').textContent = fmt(data.lstm.val_mse, 6);
            document.getElementById('lstmRMSE').textContent = fmt(data.lstm.val_rmse, 6);
        }
    } catch (e) {
        console.warn('fetchTrainingResults error:', e);
    }
}

async function renderFeatureImportance() {
    const emptyHint = document.getElementById('featureEmptyHint');
    try {
        const res = await fetch('/api/feature-importance');
        const data = await res.json();

        const xgbData = data.xgb || [];
        const lgbmData = data.lgbm || [];
        if (xgbData.length === 0) {
            if (emptyHint) emptyHint.hidden = false;
            return;
        }
        if (emptyHint) emptyHint.hidden = true;

        const labels = xgbData.map((d) => d.feature);
        const xgbValues = xgbData.map((d) => d.importance);
        const lgbmMap = {};
        lgbmData.forEach((d) => {
            lgbmMap[d.feature] = d.importance;
        });
        const lgbmValues = labels.map((l) => lgbmMap[l] || 0);

        const ctx = document.getElementById('featureChart').getContext('2d');
        if (featureChart) featureChart.destroy();

        featureChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'XGBoost',
                        data: xgbValues,
                        backgroundColor: 'rgba(37, 99, 235, 0.75)',
                        borderRadius: 4,
                    },
                    {
                        label: 'LightGBM',
                        data: lgbmValues,
                        backgroundColor: 'rgba(124, 58, 237, 0.65)',
                        borderRadius: 4,
                    },
                ],
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'top' },
                },
                scales: {
                    x: {
                        grid: { color: CHART_COLORS.grid },
                        beginAtZero: true,
                    },
                    y: { grid: { display: false } },
                },
            },
        });
    } catch (e) {
        console.warn('renderFeatureImportance error:', e);
        if (emptyHint) emptyHint.hidden = false;
    }
}

async function renderLSTMChart() {
    try {
        const res = await fetch('/api/lstm-predictions');
        const data = await res.json();
        if (!data.actual || data.actual.length === 0) return;

        const ctx = document.getElementById('lstmChart').getContext('2d');
        if (lstmChart) lstmChart.destroy();

        lstmChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.times,
                datasets: [
                    {
                        label: 'Actual',
                        data: data.actual,
                        borderColor: CHART_COLORS.primary,
                        backgroundColor: CHART_COLORS.primaryFill,
                        borderWidth: 2,
                        pointRadius: 0,
                        fill: true,
                        tension: 0.3,
                    },
                    {
                        label: 'LSTM Predicted',
                        data: data.predicted,
                        borderColor: CHART_COLORS.secondary,
                        borderWidth: 2,
                        borderDash: [6, 4],
                        pointRadius: 0,
                        fill: false,
                        tension: 0.3,
                    },
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    legend: { position: 'top' },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => ctx.dataset.label + ': ' + fmtPrice(ctx.parsed.y),
                        },
                    },
                },
                scales: {
                    x: {
                        grid: { color: CHART_COLORS.grid },
                        ticks: { maxTicksLimit: 10, maxRotation: 0 },
                    },
                    y: {
                        grid: { color: CHART_COLORS.grid },
                        ticks: {
                            callback: (v) => '$' + Number(v).toLocaleString(),
                        },
                    },
                },
            },
        });
    } catch (e) {
        console.warn('renderLSTMChart error:', e);
    }
}

async function renderLSTMLoss() {
    const lossHint = document.getElementById('lstmLossHint');
    try {
        const res = await fetch('/api/lstm-history');
        const data = await res.json();
        if (!data.loss || data.loss.length === 0) {
            if (lossHint) lossHint.hidden = false;
            return;
        }
        if (lossHint) lossHint.hidden = true;

        const epochs = data.loss.map((_, i) => 'Epoch ' + (i + 1));
        const ctx = document.getElementById('lstmLossChart').getContext('2d');
        if (lstmLossChart) lstmLossChart.destroy();

        lstmLossChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: epochs,
                datasets: [
                    {
                        label: 'Training loss',
                        data: data.loss,
                        borderColor: CHART_COLORS.primary,
                        borderWidth: 2,
                        pointRadius: 2,
                        tension: 0.3,
                    },
                    {
                        label: 'Validation loss',
                        data: data.val_loss,
                        borderColor: CHART_COLORS.danger,
                        borderWidth: 2,
                        pointRadius: 2,
                        borderDash: [6, 4],
                        tension: 0.3,
                    },
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { position: 'top' } },
                scales: {
                    x: { grid: { color: CHART_COLORS.grid } },
                    y: {
                        grid: { color: CHART_COLORS.grid },
                        title: { display: true, text: 'MSE', color: CHART_COLORS.text },
                    },
                },
            },
        });
    } catch (e) {
        console.warn('renderLSTMLoss error:', e);
        if (lossHint) lossHint.hidden = false;
    }
}

async function renderAnomalies() {
    try {
        const res = await fetch('/api/anomalies');
        const data = await res.json();

        document.getElementById('totalAnomalies').textContent =
            data.total_anomalies !== undefined ? data.total_anomalies : '--';
        if (data.total_records && data.total_anomalies !== undefined) {
            const rate = (data.total_anomalies / data.total_records) * 100;
            document.getElementById('anomalyRate').textContent = rate.toFixed(1) + '%';
        } else {
            document.getElementById('anomalyRate').textContent = '--';
        }

        if (data.all_scores && data.all_scores.length > 0) {
            const ctx = document.getElementById('anomalyChart').getContext('2d');
            if (anomalyChart) anomalyChart.destroy();

            const colors = data.all_labels.map((l) =>
                l === -1 ? 'rgba(220, 38, 38, 0.85)' : 'rgba(37, 99, 235, 0.35)'
            );
            const radii = data.all_labels.map((l) => (l === -1 ? 5 : 2));

            anomalyChart = new Chart(ctx, {
                type: 'scatter',
                data: {
                    datasets: [
                        {
                            label: 'Anomaly score',
                            data: data.all_times.map((t, i) => ({
                                x: i,
                                y: data.all_scores[i],
                            })),
                            backgroundColor: colors,
                            pointRadius: radii,
                        },
                    ],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label: (ctx) => {
                                    const i = ctx.dataIndex;
                                    const lbl =
                                        data.all_labels[i] === -1 ? ' (anomaly)' : ' (normal)';
                                    return 'Score: ' + fmt(ctx.parsed.y, 4) + lbl;
                                },
                            },
                        },
                    },
                    scales: {
                        x: {
                            title: { display: true, text: 'Chỉ số thời gian (200 điểm gần nhất)', color: CHART_COLORS.text },
                            grid: { color: CHART_COLORS.grid },
                        },
                        y: {
                            title: {
                                display: true,
                                text: 'Score (thấp hơn = bất thường hơn)',
                                color: CHART_COLORS.text,
                            },
                            grid: { color: CHART_COLORS.grid },
                        },
                    },
                },
            });
        }

        const tbody = document.getElementById('anomalyTableBody');
        if (data.anomalies && data.anomalies.length > 0) {
            tbody.innerHTML = data.anomalies
                .slice(0, 15)
                .map(
                    (a) => `
                <tr>
                    <td>${a.candle_time}</td>
                    <td>${fmtPrice(a.close)}</td>
                    <td>${fmt(a.volume, 4)}</td>
                    <td class="${Math.abs(a.price_change_pct) > 0.01 ? 'cell-warn' : ''}">${fmt(a.price_change_pct * 100, 2)}%</td>
                    <td>${fmt(a.anomaly_score, 4)}</td>
                </tr>
            `
                )
                .join('');
        } else {
            tbody.innerHTML =
                '<tr><td colspan="5" class="empty-row">Không có dòng được gắn anomaly (hoặc chưa đủ dữ liệu)</td></tr>';
        }
    } catch (e) {
        console.warn('renderAnomalies error:', e);
    }
}

async function renderPriceChart() {
    try {
        const res = await fetch('/api/price-history');
        const data = await res.json();
        if (!data.prices || data.prices.length === 0) return;

        const ctx = document.getElementById('priceChart').getContext('2d');
        if (priceChart) priceChart.destroy();

        const datasets = [
            {
                label: 'Close',
                data: data.prices,
                borderColor: CHART_COLORS.primary,
                backgroundColor: CHART_COLORS.primaryFill,
                borderWidth: 2,
                pointRadius: 0,
                fill: true,
                tension: 0.2,
            },
        ];

        if (data.ma_7 && data.ma_7.length > 0) {
            datasets.push({
                label: 'MA7',
                data: data.ma_7,
                borderColor: CHART_COLORS.success,
                borderWidth: 1.5,
                pointRadius: 0,
                borderDash: [4, 3],
                tension: 0.3,
            });
        }

        if (data.ma_20 && data.ma_20.length > 0) {
            datasets.push({
                label: 'MA20',
                data: data.ma_20,
                borderColor: CHART_COLORS.secondary,
                borderWidth: 1.5,
                pointRadius: 0,
                borderDash: [6, 3],
                tension: 0.3,
            });
        }

        priceChart = new Chart(ctx, {
            type: 'line',
            data: { labels: data.times, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    legend: { position: 'top' },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => ctx.dataset.label + ': ' + fmtPrice(ctx.parsed.y),
                        },
                    },
                },
                scales: {
                    x: {
                        ticks: { maxTicksLimit: 12, maxRotation: 0 },
                        grid: { color: CHART_COLORS.grid },
                    },
                    y: {
                        grid: { color: CHART_COLORS.grid },
                        ticks: {
                            callback: (v) => '$' + Number(v).toLocaleString(),
                        },
                    },
                },
            },
        });
    } catch (e) {
        console.warn('renderPriceChart error:', e);
    }
}

function connectSSE() {
    const evtSource = new EventSource('/stream');
    evtSource.onmessage = function (event) {
        try {
            const data = JSON.parse(event.data);
            updatePredictionUI(data);
            setModelHints(data);
        } catch (e) {
            console.warn('[SSE] Parse error:', e);
        }
    };
    evtSource.onerror = function () {
        evtSource.close();
        setTimeout(connectSSE, 5000);
    };
}

function initReportChrome() {
    const tabs = document.querySelectorAll('.pb-tab');
    const scrollEl = document.getElementById('pbMainScroll');

    tabs.forEach((tab) => {
        tab.addEventListener('click', () => {
            const sel = tab.getAttribute('data-target');
            const target = sel ? document.querySelector(sel) : null;
            if (target && scrollEl) {
                scrollEl.scrollTo({
                    top: target.offsetTop - 8,
                    behavior: 'smooth',
                });
            } else if (target) {
                target.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }
            tabs.forEach((t) => {
                t.classList.toggle('is-active', t === tab);
                t.setAttribute('aria-selected', t === tab ? 'true' : 'false');
            });
        });
    });

    document.getElementById('btnRefresh')?.addEventListener('click', async () => {
        const btn = document.getElementById('btnRefresh');
        if (btn) btn.disabled = true;
        try {
            await Promise.all([
                fetchPredictions(),
                fetchTrainingResults(),
                renderFeatureImportance(),
                renderLSTMChart(),
                renderLSTMLoss(),
                renderAnomalies(),
                renderPriceChart(),
            ]);
        } finally {
            if (btn) btn.disabled = false;
        }
    });

    const toggleBtn = document.getElementById('btnToggleFilters');
    toggleBtn?.addEventListener('click', () => {
        document.body.classList.toggle('filter-collapsed');
        const collapsed = document.body.classList.contains('filter-collapsed');
        toggleBtn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
    });
}

document.addEventListener('DOMContentLoaded', async () => {
    initReportChrome();

    await Promise.all([
        fetchPredictions(),
        fetchTrainingResults(),
        renderFeatureImportance(),
        renderLSTMChart(),
        renderLSTMLoss(),
        renderAnomalies(),
        renderPriceChart(),
    ]);

    connectSSE();

    const CHART_REFRESH_MS = 15 * 60 * 1000;
    setInterval(async () => {
        await Promise.all([renderLSTMChart(), renderAnomalies(), renderPriceChart()]);
    }, CHART_REFRESH_MS);
});
