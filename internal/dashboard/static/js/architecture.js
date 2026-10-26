const architectureDiagramRenderIdentifier = "architecture-diagram-graph";

// Fetches the embedded mermaid source for the architecture diagram.
async function fetchArchitectureDiagramSource() {
    try {
        const response = await fetch("/dashboard/architecture.mmd");
        if (!response.ok) {
            console.error("the architecture diagram source request failed with status " + response.status);
            return null;
        }
        return await response.text();
    } catch (error) {
        console.error("the architecture diagram source could not be fetched", error);
        return null;
    }
}

// Renders the diagram source into the page container as inline SVG markup.
async function renderArchitectureDiagram(diagramSource) {
    const renderedDiagram = await mermaid.render(architectureDiagramRenderIdentifier, diagramSource);
    const diagramContainer = document.getElementById("architecture-diagram");
    diagramContainer.innerHTML = renderedDiagram.svg;
    if (renderedDiagram.bindFunctions) {
        renderedDiagram.bindFunctions(diagramContainer);
    }
}

// Reveals the status line explaining why the diagram is unavailable.
function showDiagramStatus(statusMessage) {
    const statusElement = document.getElementById("diagram-status");
    statusElement.textContent = statusMessage;
    statusElement.classList.remove("hidden");
}

// Loads and renders the architecture diagram once the page scripts run.
async function startArchitectureDiagram() {
    mermaid.initialize({ startOnLoad: false, theme: "neutral", flowchart: { useMaxWidth: false } });
    const diagramSource = await fetchArchitectureDiagramSource();
    if (diagramSource === null) {
        showDiagramStatus("the architecture diagram could not be loaded");
        return;
    }
    try {
        await renderArchitectureDiagram(diagramSource);
    } catch (error) {
        console.error("the architecture diagram could not be rendered", error);
        showDiagramStatus("the architecture diagram could not be rendered");
    }
}

startArchitectureDiagram();
