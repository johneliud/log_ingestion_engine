const metricsPollingIntervalMilliseconds = 5000;

// Fetches one metrics snapshot from the JSON endpoint, or null when the request fails.
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

// Updates the summary and counter cards from one snapshot.
function updateCounterCards(snapshot) {
    setPlainValue("total-logs-received", snapshot.total_logs_received.toLocaleString("en-US"));
    setPlainValue("logs-per-second", snapshot.throughput.toFixed(2));
    setPlainValue("error-rate", (snapshot.error_rate * 100).toFixed(2) + "%");
    setPlainValue("queue-backlog", snapshot.queue_backlog.toLocaleString("en-US"));
    setPlainValue("rate-limited-requests", snapshot.rate_limited_requests.toLocaleString("en-US"));
    setPlainValue("dead-lettered-logs", snapshot.dead_lettered_logs.toLocaleString("en-US"));
    setPlainValue("write-failures", snapshot.write_failures.toLocaleString("en-US"));
}

// Builds one table row with a name and its formatted count.
function metricTableRow(metricName, metricCount) {
    const row = document.createElement("tr");
    row.className = "border-t border-slate-200";
    const nameCell = document.createElement("td");
    nameCell.className = "py-1";
    nameCell.textContent = metricName;
    const countCell = document.createElement("td");
    countCell.className = "py-1 text-right";
    countCell.textContent = metricCount.toLocaleString("en-US");
    row.append(nameCell, countCell);
    return row;
}

// Builds the placeholder row shown before any log has been received.
function emptyTableRow(rowMessage) {
    const row = document.createElement("tr");
    const messageCell = document.createElement("td");
    messageCell.className = "py-1";
    messageCell.colSpan = 2;
    messageCell.textContent = rowMessage;
    row.append(messageCell);
    return row;
}

// Replaces one table body with the rows for the given entries.
function replaceTableRows(tableBodyID, tableRows) {
    const tableBody = document.getElementById(tableBodyID);
    if (tableRows.length === 0) {
        tableBody.replaceChildren(emptyTableRow("no logs received yet"));
        return;
    }
    tableBody.replaceChildren(...tableRows);
}

// Reveals the status line explaining why the snapshot is unavailable.
function showMetricsStatus(statusMessage) {
    const statusElement = document.getElementById("metrics-status");
    statusElement.textContent = statusMessage;
    statusElement.classList.remove("hidden");
}

// Fetches one snapshot and updates every value on the page.
async function refreshMetricsPage() {
    const snapshot = await fetchMetricsSnapshot();
    if (snapshot === null) {
        showMetricsStatus("the metrics snapshot could not be loaded");
        return;
    }
    updateCounterCards(snapshot);
    const levelRows = Object.entries(snapshot.logs_by_level).map(([levelName, levelCount]) => metricTableRow(levelName, levelCount));
    replaceTableRows("logs-by-level-table", levelRows);
    const serviceRows = Object.entries(snapshot.logs_by_service)
        .sort((leftService, rightService) => rightService[1] - leftService[1])
        .map(([serviceName, serviceCount]) => metricTableRow(serviceName, serviceCount));
    replaceTableRows("logs-by-service-table", serviceRows);
    setPlainValue("last-refreshed", "refreshed at " + new Date().toLocaleTimeString());
}

// Starts the five-second polling loop with an immediate first refresh.
function startMetricsPolling() {
    refreshMetricsPage();
    setInterval(refreshMetricsPage, metricsPollingIntervalMilliseconds);
}

startMetricsPolling();
