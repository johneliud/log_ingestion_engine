const pollingIntervalMilliseconds = 5000;
const maximumHistoryPoints = 120;
const chartGridColor = "#e2e8f0";
const chartTextColor = "#475569";
const throughputLineColor = "#0284c7";
const topServicesBarColor = "#4f46e5";
const historyTimestampLabels = [];
const logsPerSecondHistory = [];
const errorRateHistory = [];
let throughputChartInstance = null;
let errorRateChartInstance = null;
let topServicesChartInstance = null;

// Returns one metrics snapshot from the JSON endpoint, or null when the poll fails.
async function fetchMetricsSnapshot() {
    try {
        const response = await fetch("/metrics.json");
        if (!response.ok) {
            return null;
        }
        return await response.json();
    } catch (error) {
        return null;
    }
}

// Replaces the text of one page element with a formatted value.
function setPlainValue(elementID, formattedValue) {
    document.getElementById(elementID).textContent = formattedValue;
}

// Updates the four stat cards from one snapshot, formatting each required value as a plain number.
function updatePlainValueCards(snapshot) {
    setPlainValue("total-logs-received", snapshot.total_logs_received.toLocaleString("en-US"));
    setPlainValue("logs-per-second", snapshot.throughput.toFixed(2));
    setPlainValue("error-rate", (snapshot.error_rate * 100).toFixed(2) + "%");
    setPlainValue("queue-backlog", snapshot.queue_backlog.toLocaleString("en-US"));
}

// Appends the snapshot to the bounded client-side history, dropping the oldest point when full.
function appendSnapshotToHistory(snapshot) {
    historyTimestampLabels.push(new Date().toLocaleTimeString());
    logsPerSecondHistory.push(snapshot.throughput);
    errorRateHistory.push(snapshot.error_rate);
    while (logsPerSecondHistory.length > maximumHistoryPoints) {
        historyTimestampLabels.shift();
        logsPerSecondHistory.shift();
        errorRateHistory.shift();
    }
}

// Returns the five highest-volume services from the cumulative per-service counts.
function topFiveServicesFrom(logsByService) {
    return Object.entries(logsByService)
        .map(([serviceName, receivedCount]) => ({ name: serviceName, count: receivedCount }))
        .sort((leftService, rightService) => rightService.count - leftService.count)
        .slice(0, 5);
}

// Builds one top-services table row with the service name and its plain-number count.
function topServiceRow(topService) {
    const row = document.createElement("tr");
    row.className = "border-t border-slate-200";
    const nameCell = document.createElement("td");
    nameCell.className = "py-1";
    nameCell.textContent = topService.name;
    const countCell = document.createElement("td");
    countCell.className = "py-1 text-right";
    countCell.textContent = topService.count.toLocaleString("en-US");
    row.append(nameCell, countCell);
    return row;
}

// Builds the placeholder row shown before any log has been received.
function emptyTopServicesRow(rowMessage) {
    const row = document.createElement("tr");
    const messageCell = document.createElement("td");
    messageCell.className = "py-1";
    messageCell.colSpan = 2;
    messageCell.textContent = rowMessage;
    row.append(messageCell);
    return row;
}

// Renders the top-five service table beneath the bar chart.
function updateTopServicesTable(topServices) {
    const tableBody = document.getElementById("top-services-table");
    if (topServices.length === 0) {
        tableBody.replaceChildren(emptyTopServicesRow("no logs received yet"));
        return;
    }
    tableBody.replaceChildren(...topServices.map(topServiceRow));
}

// Creates the throughput line chart on first refresh and updates it from the shared history afterwards.
function updateThroughputChart() {
    if (throughputChartInstance !== null) {
        throughputChartInstance.update();
        return;
    }
    throughputChartInstance = new Chart(document.getElementById("throughput-chart"), {
        type: "line",
        data: {
            labels: historyTimestampLabels,
            datasets: [{
                label: "logs per second",
                data: logsPerSecondHistory,
                borderColor: throughputLineColor,
                backgroundColor: "rgba(2, 132, 199, 0.15)",
                fill: true,
                tension: 0.25,
            }],
        },
        options: {
            animation: false,
            scales: {
                x: { ticks: { color: chartTextColor, maxTicksLimit: 8 }, grid: { color: chartGridColor } },
                y: { beginAtZero: true, ticks: { color: chartTextColor }, grid: { color: chartGridColor } },
            },
            plugins: { legend: { labels: { color: "#1e293b" } } },
        },
    });
}

// Creates the error-rate line chart on first refresh and updates it from the shared history afterwards.
function updateErrorRateChart() {
    if (errorRateChartInstance !== null) {
        errorRateChartInstance.update();
        return;
    }
    errorRateChartInstance = new Chart(document.getElementById("error-rate-chart"), {
        type: "line",
        data: {
            labels: historyTimestampLabels,
            datasets: [{
                label: "error rate",
                data: errorRateHistory,
                borderColor: "#dc2626",
                backgroundColor: "rgba(220, 38, 38, 0.15)",
                fill: true,
                tension: 0.25,
            }],
        },
        options: {
            animation: false,
            scales: {
                x: { ticks: { color: chartTextColor, maxTicksLimit: 8 }, grid: { color: chartGridColor } },
                y: { beginAtZero: true, ticks: { color: chartTextColor }, grid: { color: chartGridColor } },
            },
            plugins: { legend: { labels: { color: "#1e293b" } } },
        },
    });
}

// Creates the top-services bar chart on first refresh and points it at the latest top-five list afterwards.
function updateTopServicesChart(topServices) {
    const serviceNames = topServices.map((topService) => topService.name);
    const serviceCounts = topServices.map((topService) => topService.count);
    if (topServicesChartInstance !== null) {
        topServicesChartInstance.data.labels = serviceNames;
        topServicesChartInstance.data.datasets[0].data = serviceCounts;
        topServicesChartInstance.update();
        return;
    }
    topServicesChartInstance = new Chart(document.getElementById("top-services-chart"), {
        type: "bar",
        data: {
            labels: serviceNames,
            datasets: [{ label: "logs received", data: serviceCounts, backgroundColor: topServicesBarColor }],
        },
        options: {
            animation: false,
            indexAxis: "y",
            scales: {
                x: { beginAtZero: true, ticks: { color: chartTextColor }, grid: { color: chartGridColor } },
                y: { ticks: { color: chartTextColor }, grid: { color: chartGridColor } },
            },
            plugins: { legend: { display: false } },
        },
    });
}

// Records the wall-clock time of a successful poll on the page.
function updateLastRefreshed() {
    setPlainValue("last-refreshed", "refreshed at " + new Date().toLocaleTimeString());
}

// Fetches one snapshot and updates every chart and plain number on the page.
async function refreshDashboardFromSnapshot() {
    const snapshot = await fetchMetricsSnapshot();
    if (snapshot === null) {
        return;
    }
    updatePlainValueCards(snapshot);
    appendSnapshotToHistory(snapshot);
    const topServices = topFiveServicesFrom(snapshot.logs_by_service);
    updateTopServicesTable(topServices);
    updateThroughputChart();
    updateErrorRateChart();
    updateTopServicesChart(topServices);
    updateLastRefreshed();
}

// Starts the five-second polling loop with an immediate first refresh.
function startDashboardPolling() {
    refreshDashboardFromSnapshot();
    setInterval(refreshDashboardFromSnapshot, pollingIntervalMilliseconds);
}

startDashboardPolling();
