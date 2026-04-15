/**
 * app.js — Crypto ML Dashboard Frontend
 * ======================================
 * - Fetch data từ Flask API
 * - Render Chart.js charts
 * - SSE real-time updates mỗi 30s
 */

// ── Chart.js defaults ───────────────────────────────────────────
Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = 'rgba(99,102,241,0.1)';
Chart.defaults.font.family = "'Inter', sans-serif";

// ── Chart instances (để update sau) ─────────────────────────────
let featureChart = null;
let lstmChart = null;
let lstmLossChart = null;
let anomalyChart = null;
let priceChart = null;

// ── Helper: format số ───────────────────────────────────────────
function fmt(n, decimals = 2) {
    if (n === null || n === undefined || isNaN(n)) return '--';
    return Number(n).toLocaleString('en-US', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
}

function fmtPrice(n) {
    if (n === null || n === undefined) return '--';
    return '$' + fmt(n, 2);
}

// ══════════════════════════════════════════════════════════════════
// 1. FETCH PREDICTIONS (real-time)
// ══════════════════════════════════════════════════════════════════
async function fetchPredictions() {
    try {
        const res = await fetch('/api/predictions');
        const data = await res.json();
        updatePredictionUI(data);
    } catch (e) {
        console.warn('fetchPredictions error:', e);
    }
}

function updatePredictionUI(data) {
    // Ticker
    const price = data.current_price;
    document.getElementById('tickerPrice').textContent = fmtPrice(price);

    const changeEl = document.getElementById('tickerChange');
    if (data.lstm_change_pct !== null && data.lstm_change_pct !== undefined) {
        const pct = data.lstm_change_pct;
        changeEl.textContent = (pct >= 0 ? '+' : '') + fmt(pct, 2) + '%';
        changeEl.className = 'ticker-change ' + (pct >= 0 ? 'up' : 'down');
    }

    document.getElementById('lastUpdate').textContent =
        'Cập nhật: ' + new Date().toLocaleTimeString('vi-VN');

    // XGBoost
    const xgbSignal = document.getElementById('xgbSignal');
    xgbSignal.textContent = data.xgboost_signal || '--';
    xgbSignal.className = 'signal-value ' + (data.xgboost_signal === 'BULLISH' ? 'bull' : 'bear');
    document.getElementById('xgbProb').textContent =
        'Probability: ' + (data.xgboost_bull_prob !== null ? fmt(data.xgboost_bull_prob * 100, 1) + '%' : '--');
    const xgbBox = document.getElementById('xgbSignalBox');
    xgbBox.className = 'signal-box ' + (data.xgboost_signal === 'BULLISH' ? 'bullish' : 'bearish');

    // LightGBM
    const lgbmSignal = document.getElementById('lgbmSignal');
    lgbmSignal.textContent = data.lgbm_signal || '--';
    lgbmSignal.className = 'signal-value ' + (data.lgbm_signal === 'BULLISH' ? 'bull' : 'bear');
    document.getElementById('lgbmProb').textContent =
        'Probability: ' + (data.lgbm_bull_prob !== null ? fmt(data.lgbm_bull_prob * 100, 1) + '%' : '--');
    const lgbmBox = document.getElementById('lgbmSignalBox');
    lgbmBox.className = 'signal-box ' + (data.lgbm_signal === 'BULLISH' ? 'bullish' : 'bearish');

    // LSTM
    document.getElementById('lstmCurrent').textContent = fmtPrice(price);
    document.getElementById('lstmPredicted').textContent = fmtPrice(data.lstm_predicted_price);
    const changePct = document.getElementById('lstmChangePct');
    if (data.lstm_change_pct !== null && data.lstm_change_pct !== undefined) {
        const pct = data.lstm_change_pct;
        changePct.textContent = (pct >= 0 ? '▲ +' : '▼ ') + fmt(pct, 4) + '%';
        changePct.className = 'stat-sub ' + (pct >= 0 ? 'up' : 'down');
    }

    const predBox = document.getElementById('lstmPredBox');
    if (data.lstm_predicted_price && price) {
        predBox.style.borderColor = data.lstm_predicted_price > price
            ? 'rgba(34,197,94,0.4)' : 'rgba(239,68,68,0.4)';
    }

    // Anomaly
    const statusEl = document.getElementById('anomalyStatus');
    const statusBox = document.getElementById('anomalyStatusBox');
    if (data.is_anomaly) {
        statusEl.textContent = '⚠️ ANOMALY';
        statusEl.style.color = '#ef4444';
        statusBox.className = 'stat-box anomaly';
    } else {
        statusEl.textContent = '✅ NORMAL';
        statusEl.style.color = '#22c55e';
        statusBox.className = 'stat-box normal';
    }
    document.getElementById('anomalyScore').textContent =
        data.anomaly_score !== null ? fmt(data.anomaly_score, 4) : '--';
}

// ══════════════════════════════════════════════════════════════════
// 2. TRAINING RESULTS
// ══════════════════════════════════════════════════════════════════
async function fetchTrainingResults() {
    try {
        const res = await fetch('/api/training-results');
        const data = await res.json();

        if (data.xgboost) {
            document.getElementById('xgbAcc').textContent = fmt(data.xgboost.accuracy * 100, 1) + '%';
            document.getElementById('xgbPrec').textContent = fmt(data.xgboost.precision * 100, 1) + '%';
            document.getElementById('xgbRec').textContent = fmt(data.xgboost.recall * 100, 1) + '%';
            document.getElementById('xgbF1').textContent = fmt(data.xgboost.f1 * 100, 1) + '%';
        }
        if (data.lightgbm) {
            document.getElementById('lgbmAcc').textContent = fmt(data.lightgbm.accuracy * 100, 1) + '%';
            document.getElementById('lgbmPrec').textContent = fmt(data.lightgbm.precision * 100, 1) + '%';
            document.getElementById('lgbmRec').textContent = fmt(data.lightgbm.recall * 100, 1) + '%';
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

// ══════════════════════════════════════════════════════════════════
// 3. FEATURE IMPORTANCE CHART
// ══════════════════════════════════════════════════════════════════
async function renderFeatureImportance() {
    try {
        const res = await fetch('/api/feature-importance');
        const data = await res.json();

        const xgbData = data.xgb || [];
        const lgbmData = data.lgbm || [];
        if (xgbData.length === 0) return;

        const labels = xgbData.map(d => d.feature);
        const xgbValues = xgbData.map(d => d.importance);
        const lgbmMap = {};
        lgbmData.forEach(d => { lgbmMap[d.feature] = d.importance; });
        const lgbmValues = labels.map(l => lgbmMap[l] || 0);

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
                        backgroundColor: 'rgba(99,102,241,0.7)',
                        borderRadius: 4,
                    },
                    {
                        label: 'LightGBM',
                        data: lgbmValues,
                        backgroundColor: 'rgba(168,85,247,0.7)',
                        borderRadius: 4,
                    }
                ]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'top' }
                },
                scales: {
                    x: { grid: { color: 'rgba(99,102,241,0.05)' } },
                    y: { grid: { display: false } }
                }
            }
        });
    } catch (e) {
        console.warn('renderFeatureImportance error:', e);
    }
}

// ══════════════════════════════════════════════════════════════════
// 4. LSTM ACTUAL VS PREDICTED CHART
// ══════════════════════════════════════════════════════════════════
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
                        label: 'Actual Price',
                        data: data.actual,
                        borderColor: '#6366f1',
                        backgroundColor: 'rgba(99,102,241,0.1)',
                        borderWidth: 2,
                        pointRadius: 0,
                        fill: true,
                        tension: 0.3,
                    },
                    {
                        label: 'LSTM Predicted',
                        data: data.predicted,
                        borderColor: '#f59e0b',
                        borderWidth: 2,
                        borderDash: [5, 3],
                        pointRadius: 0,
                        fill: false,
                        tension: 0.3,
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    legend: { position: 'top' },
                    tooltip: {
                        callbacks: {
                            label: ctx => ctx.dataset.label + ': ' + fmtPrice(ctx.parsed.y)
                        }
                    }
                },
                scales: {
                    x: {
                        display: true,
                        ticks: { maxTicksLimit: 10, maxRotation: 0 },
                        grid: { color: 'rgba(99,102,241,0.05)' }
                    },
                    y: {
                        grid: { color: 'rgba(99,102,241,0.05)' },
                        ticks: { callback: v => '$' + v.toLocaleString() }
                    }
                }
            }
        });
    } catch (e) {
        console.warn('renderLSTMChart error:', e);
    }
}

// ══════════════════════════════════════════════════════════════════
// 5. LSTM TRAINING LOSS CHART
// ══════════════════════════════════════════════════════════════════
async function renderLSTMLoss() {
    try {
        const res = await fetch('/api/lstm-history');
        const data = await res.json();
        if (!data.loss || data.loss.length === 0) return;

        const epochs = data.loss.map((_, i) => 'Epoch ' + (i + 1));
        const ctx = document.getElementById('lstmLossChart').getContext('2d');
        if (lstmLossChart) lstmLossChart.destroy();

        lstmLossChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: epochs,
                datasets: [
                    {
                        label: 'Training Loss',
                        data: data.loss,
                        borderColor: '#6366f1',
                        borderWidth: 2,
                        pointRadius: 3,
                        tension: 0.3,
                    },
                    {
                        label: 'Validation Loss',
                        data: data.val_loss,
                        borderColor: '#ef4444',
                        borderWidth: 2,
                        pointRadius: 3,
                        borderDash: [5, 3],
                        tension: 0.3,
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { position: 'top' } },
                scales: {
                    x: { grid: { color: 'rgba(99,102,241,0.05)' } },
                    y: { grid: { color: 'rgba(99,102,241,0.05)' }, title: { display: true, text: 'MSE Loss' } }
                }
            }
        });
    } catch (e) {
        console.warn('renderLSTMLoss error:', e);
    }
}

// ══════════════════════════════════════════════════════════════════
// 6. ANOMALY CHART + TABLE
// ══════════════════════════════════════════════════════════════════
async function renderAnomalies() {
    try {
        const res = await fetch('/api/anomalies');
        const data = await res.json();

        // Stats
        document.getElementById('totalAnomalies').textContent = data.total_anomalies || '--';
        if (data.total_records && data.total_anomalies) {
            const rate = (data.total_anomalies / data.total_records * 100).toFixed(1);
            document.getElementById('anomalyRate').textContent = rate + '%';
        }

        // Chart
        if (data.all_scores && data.all_scores.length > 0) {
            const ctx = document.getElementById('anomalyChart').getContext('2d');
            if (anomalyChart) anomalyChart.destroy();

            const colors = data.all_labels.map(l =>
                l === -1 ? 'rgba(239,68,68,0.8)' : 'rgba(99,102,241,0.4)');
            const radii = data.all_labels.map(l => l === -1 ? 5 : 2);

            anomalyChart = new Chart(ctx, {
                type: 'scatter',
                data: {
                    datasets: [{
                        label: 'Anomaly Score',
                        data: data.all_times.map((t, i) => ({
                            x: i,
                            y: data.all_scores[i]
                        })),
                        backgroundColor: colors,
                        pointRadius: radii,
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label: ctx => 'Score: ' + fmt(ctx.parsed.y, 4) +
                                    (data.all_labels[ctx.dataIndex] === -1 ? ' ⚠️ ANOMALY' : ' ✅ Normal')
                            }
                        }
                    },
                    scales: {
                        x: {
                            title: { display: true, text: 'Time Index' },
                            grid: { color: 'rgba(99,102,241,0.05)' }
                        },
                        y: {
                            title: { display: true, text: 'Anomaly Score (thấp = bất thường hơn)' },
                            grid: { color: 'rgba(99,102,241,0.05)' }
                        }
                    }
                }
            });
        }

        // Table
        const tbody = document.getElementById('anomalyTableBody');
        if (data.anomalies && data.anomalies.length > 0) {
            tbody.innerHTML = data.anomalies.slice(0, 15).map(a => `
                <tr>
                    <td>${a.candle_time}</td>
                    <td>${fmtPrice(a.close)}</td>
                    <td>${fmt(a.volume, 4)}</td>
                    <td style="color: ${Math.abs(a.price_change_pct) > 0.01 ? '#ef4444' : '#94a3b8'}">${fmt(a.price_change_pct * 100, 2)}%</td>
                    <td>${fmt(a.anomaly_score, 4)}</td>
                </tr>
            `).join('');
        } else {
            tbody.innerHTML = '<tr><td colspan="5" class="empty-row">Không tìm thấy anomaly</td></tr>';
        }
    } catch (e) {
        console.warn('renderAnomalies error:', e);
    }
}

// ══════════════════════════════════════════════════════════════════
// 7. PRICE CHART WITH MA
// ══════════════════════════════════════════════════════════════════
async function renderPriceChart() {
    try {
        const res = await fetch('/api/price-history');
        const data = await res.json();
        if (!data.prices || data.prices.length === 0) return;

        const ctx = document.getElementById('priceChart').getContext('2d');
        if (priceChart) priceChart.destroy();

        const datasets = [
            {
                label: 'Close Price',
                data: data.prices,
                borderColor: '#6366f1',
                backgroundColor: 'rgba(99,102,241,0.08)',
                borderWidth: 2,
                pointRadius: 0,
                fill: true,
                tension: 0.2,
            }
        ];

        if (data.ma_7 && data.ma_7.length > 0) {
            datasets.push({
                label: 'MA7',
                data: data.ma_7,
                borderColor: '#22c55e',
                borderWidth: 1.5,
                pointRadius: 0,
                borderDash: [4, 2],
                tension: 0.3,
            });
        }

        if (data.ma_20 && data.ma_20.length > 0) {
            datasets.push({
                label: 'MA20',
                data: data.ma_20,
                borderColor: '#f59e0b',
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
                            label: ctx => ctx.dataset.label + ': ' + fmtPrice(ctx.parsed.y)
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: { maxTicksLimit: 12, maxRotation: 0 },
                        grid: { color: 'rgba(99,102,241,0.05)' }
                    },
                    y: {
                        grid: { color: 'rgba(99,102,241,0.05)' },
                        ticks: { callback: v => '$' + v.toLocaleString() }
                    }
                }
            }
        });
    } catch (e) {
        console.warn('renderPriceChart error:', e);
    }
}

// ══════════════════════════════════════════════════════════════════
// 8. SSE REAL-TIME STREAM
// ══════════════════════════════════════════════════════════════════
function connectSSE() {
    const evtSource = new EventSource('/stream');
    evtSource.onmessage = function (event) {
        try {
            const data = JSON.parse(event.data);
            updatePredictionUI(data);
            console.log('[SSE] Update received:', data.timestamp);
        } catch (e) {
            console.warn('[SSE] Parse error:', e);
        }
    };
    evtSource.onerror = function () {
        console.warn('[SSE] Connection lost, reconnecting in 5s...');
        evtSource.close();
        setTimeout(connectSSE, 5000);
    };
}

// ══════════════════════════════════════════════════════════════════
// 9. INIT
// ══════════════════════════════════════════════════════════════════
document.addEventListener('DOMContentLoaded', async () => {
    console.log('🚀 Crypto ML Dashboard loading...');

    // Fetch tất cả data song song
    await Promise.all([
        fetchPredictions(),
        fetchTrainingResults(),
        renderFeatureImportance(),
        renderLSTMChart(),
        renderLSTMLoss(),
        renderAnomalies(),
        renderPriceChart(),
    ]);

    console.log('✅ Dashboard loaded');

    // Kết nối real-time SSE
    connectSSE();

    // Refresh charts mỗi 60 giây
    setInterval(async () => {
        await Promise.all([
            renderLSTMChart(),
            renderAnomalies(),
            renderPriceChart(),
        ]);
    }, 60000);
});
