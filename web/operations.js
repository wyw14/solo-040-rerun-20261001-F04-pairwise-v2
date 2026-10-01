const { api, shortID, setMessage, withBusy } = window.LockFlow;
const form = document.querySelector("#plan-form");
const message = document.querySelector("#plan-message");

const convoyVessels = [];

function renderConvoy() {
  document.querySelector("#vessel-list").innerHTML = convoyVessels.map((vessel, index) =>
    `<li>${escapeHTML(vessel.name)} <span class="muted">${escapeHTML(vessel.call_sign)} · ${vessel.length_m}m</span>
      <button type="button" class="link-btn" data-remove="${index}">remove</button></li>`).join("");
}

const STATE_LABEL = {
  pending: "Pending", publishing: "Publishing", published: "Published",
  skipped: "Already on", failed: "Failed", compensated: "Compensated",
};
const STATUS_LABEL = {
  running: "Running", succeeded: "Published", failed: "Failed", cancelled: "Cancelled",
};

const escapeHTML = (value) => String(value).replace(/[&<>"']/g, (char) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));

function vesselBadges(plan) {
  const progressByVessel = new Map();
  if (plan.publication) {
    for (const vessel of plan.publication.vessels) progressByVessel.set(vessel.vessel_id, vessel);
  }
  return plan.vessels.map((vessel) => {
    const progress = progressByVessel.get(vessel.id);
    const state = progress ? progress.state : "pending";
    const title = progress && progress.error ? progress.error : STATE_LABEL[state];
    return `<span class="badge b-${state}" title="${escapeHTML(title)}">${escapeHTML(vessel.name)} · ${STATE_LABEL[state]}</span>`;
  }).join(" ");
}

function publishCell(plan) {
  const publication = plan.publication;
  if (!publication) return `<span class="muted">Not requested</span>`;
  const total = publication.vessels.length;
  const done = publication.published + publication.skipped + publication.compensated + publication.failed;
  const statusClass = `pub-${publication.status}`;
  const error = publication.error ? `<div class="cell-error">${escapeHTML(publication.error)}</div>` : "";
  return `<span class="pub-status ${statusClass}">${STATUS_LABEL[publication.status] || publication.status}</span>
    <span class="muted"> ${done}/${total}</span>${error}`;
}

function actionCell(plan) {
  const status = plan.publication ? plan.publication.status : null;
  const running = status === "running";
  const publishDisabled = running || plan.cancelled ? "disabled" : "";
  const cancelDisabled = plan.cancelled ? "disabled" : "";
  return `<button class="publish-btn" data-plan="${plan.id}" ${publishDisabled}>Publish signals</button>
    <button class="danger cancel-btn" data-plan="${plan.id}" ${cancelDisabled}>Cancel</button>`;
}

async function refresh() {
  const [plans, chambers, health] = await Promise.all([api("/api/plans"), api("/api/chambers"), api("/healthz")]);
  document.querySelector("#health").textContent = health.status === "ok" ? "Devices online" : "Attention required";
  const rows = Object.entries(plans).flatMap(([chamber, items]) => items.map((plan) =>
    `<tr class="${plan.cancelled ? "row-cancelled" : ""}"><td>${chamber}</td><td>${shortID(plan.id)}</td><td>${plan.direction}</td>
     <td><div class="badges">${vesselBadges(plan)}</div></td><td>${publishCell(plan)}</td>
     <td>${new Date(plan.created_at).toLocaleTimeString()}</td><td class="actions">${actionCell(plan)}</td></tr>`));
  document.querySelector("#plans").innerHTML = rows.join("") || '<tr><td colspan="7" class="muted">No plans yet</td></tr>';
  const current = chambers.find((item) => item.cycle);
  document.querySelector("#metric-chamber").textContent = current?.chamber_id || "Idle";
  document.querySelector("#metric-stage").textContent = current?.cycle?.stage || "-";
  document.querySelector("#metric-cycle").textContent = shortID(current?.cycle?.id);
}

async function act(button, path, options) {
  await withBusy(button, async () => {
    try {
      const result = await api(path, options);
      const status = result.publication ? result.publication.status : result.status;
      setMessage(message, status === "running" ? "Publication in progress…" : `Publication ${status || "accepted"}.`);
    } catch (error) {
      setMessage(message, error.message);
    } finally {
      await refresh().catch((error) => setMessage(message, error.message));
    }
  });
}

document.querySelector("#plans").addEventListener("click", (event) => {
  const publishButton = event.target.closest(".publish-btn");
  const cancelButton = event.target.closest(".cancel-btn");
  if (publishButton) {
    act(publishButton, `/api/plans/${publishButton.dataset.plan}/signals`, { method: "POST" });
  } else if (cancelButton) {
    act(cancelButton, `/api/plans/${cancelButton.dataset.plan}/cancel`, { method: "POST" });
  }
});

document.querySelector("#add-vessel").addEventListener("click", () => {
  const name = document.querySelector("#vessel").value.trim();
  const callSign = document.querySelector("#call-sign").value.trim();
  const lengthM = Number(document.querySelector("#length").value);
  if (!name || !callSign || lengthM <= 0) {
    setMessage(message, "Vessel name, call sign and positive length are required.");
    return;
  }
  convoyVessels.push({ name, call_sign: callSign, length_m: lengthM });
  document.querySelector("#vessel").value = "";
  document.querySelector("#call-sign").value = "";
  document.querySelector("#length").value = "";
  setMessage(message, "");
  renderConvoy();
});

document.querySelector("#vessel-list").addEventListener("click", (event) => {
  const button = event.target.closest("[data-remove]");
  if (!button) return;
  convoyVessels.splice(Number(button.dataset.remove), 1);
  renderConvoy();
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  // Treat the current form row as one more vessel when it is filled in.
  const name = document.querySelector("#vessel").value.trim();
  const callSign = document.querySelector("#call-sign").value.trim();
  const lengthM = Number(document.querySelector("#length").value);
  const vessels = convoyVessels.slice();
  if (name || callSign || document.querySelector("#length").value !== "") {
    if (!name || !callSign || lengthM <= 0) {
      setMessage(message, "Vessel name, call sign and positive length are required.");
      return;
    }
    vessels.push({ name, call_sign: callSign, length_m: lengthM });
  }
  if (vessels.length === 0) {
    setMessage(message, "Add at least one vessel to the convoy.");
    return;
  }
  const button = form.querySelector("button[type=submit]");
  await withBusy(button, async () => {
    try {
      const result = await api("/api/plans", { method: "POST", body: JSON.stringify({
        chamber_id: document.querySelector("#chamber").value,
        direction: document.querySelector("#direction").value,
        vessels,
      }) });
      convoyVessels.length = 0;
      renderConvoy();
      setMessage(message, `Plan ${shortID(result.plan.id)} admitted to ${result.plan.chamber_id}.`);
      await refresh();
    } catch (error) { setMessage(message, error.message); }
  });
});

refresh().catch((error) => setMessage(message, error.message));
setInterval(() => refresh().catch(() => {}), 2000);
