const { api, shortID, setMessage, withBusy } = window.LockFlow;
const form = document.querySelector("#plan-form");
const message = document.querySelector("#plan-message");

const STATUS_LABEL = {
  pending: "待发布",
  publishing: "发布中",
  published: "已发布",
  failed: "失败",
  compensated: "已补偿",
};

const PUBLICATION_LABEL = {
  running: "发布中",
  completed: "发布完成",
  failed: "失败待处理",
  compensated: "已补偿回滚",
};

const escapeHTML = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => (
  { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]
));

function vesselProgress(plan, publication) {
  if (!publication) {
    return plan.vessels.map((vessel) =>
      `<span class="vessel-state is-pending">${escapeHTML(vessel.call_sign)} · 待发布</span>`).join("");
  }
  const byID = new Map(publication.vessels.map((item) => [item.vessel_id, item]));
  return plan.vessels.map((vessel) => {
    const progress = byID.get(vessel.id);
    const status = progress ? progress.status : "pending";
    const hint = progress && progress.error ? ` title="${escapeHTML(progress.error)}"` : "";
    return `<span class="vessel-state is-${status}"${hint}>${escapeHTML(vessel.call_sign)} · ${STATUS_LABEL[status] || status}${progress && progress.attempts > 1 ? ` ×${progress.attempts}` : ""}</span>`;
  }).join("");
}

function planStatus(plan, publication) {
  if (plan.cancelled) return `<span class="badge is-compensated">计划已取消</span>`;
  if (!publication) return `<span class="badge is-pending">未发布</span>`;
  return `<span class="badge is-${publication.status}">${PUBLICATION_LABEL[publication.status] || publication.status}</span>`;
}

function planActions(plan, publication) {
  const running = publication && publication.status === "running";
  const done = publication && publication.status === "completed";
  if (plan.cancelled) {
    return `<button type="button" disabled>已取消</button>`;
  }
  return `
    <button type="button" class="publish-btn" data-plan="${plan.id}"${running || done ? " disabled" : ""}>${done ? "已发布" : "发布信号"}</button>
    <button type="button" class="danger cancel-btn" data-plan="${plan.id}"${running ? " disabled" : ""}>取消计划</button>`;
}

async function refresh() {
  const [plans, chambers, health] = await Promise.all([api("/api/plans"), api("/api/chambers"), api("/healthz")]);
  document.querySelector("#health").textContent = health.status === "ok" ? "Devices online" : "Attention required";

  const publicationByPlan = new Map();
  let anyRunning = false;
  for (const chamber of chambers) {
    for (const publication of chamber.publications || []) {
      publicationByPlan.set(publication.plan_id, publication);
      if (publication.status === "running") anyRunning = true;
    }
  }

  const rows = Object.entries(plans).flatMap(([chamber, items]) => items.map((plan) => {
    const publication = publicationByPlan.get(plan.id);
    const count = publication
      ? `${publication.published.length}/${plan.vessels.length}`
      : `0/${plan.vessels.length}`;
    const publicationError = publication && publication.error
      ? `<div class="muted publication-error" title="${escapeHTML(publication.error)}">${escapeHTML(publication.error.slice(0, 48))}</div>`
      : "";
    return `<tr>
      <td>${chamber}</td>
      <td>${shortID(plan.id)}</td>
      <td>${plan.direction}</td>
      <td>${plan.vessels.length} 船
        <div class="vessel-list">${vesselProgress(plan, publication)}</div>
      </td>
      <td><div class="progress-count">${count}</div>${publicationError}</td>
      <td>${planStatus(plan, publication)}</td>
      <td>${new Date(plan.created_at).toLocaleTimeString()}</td>
      <td class="actions">${planActions(plan, publication)}</td>
    </tr>`;
  }));
  document.querySelector("#plans").innerHTML = rows.join("") || '<tr><td colspan="8" class="muted">No plans</td></tr>';

  // While a publication is in flight refresh quickly so per-vessel progress
  // visibly advances; otherwise settle back to the normal cadence.
  refreshTimer.delay = anyRunning ? 1500 : 5000;
}

const refreshTimer = { delay: 5000 };
async function refreshLoop() {
  await refresh().catch((error) => setMessage(message, error.message));
  setTimeout(refreshLoop, refreshTimer.delay);
}

document.querySelector("#plans").addEventListener("click", async (event) => {
  const publishButton = event.target.closest(".publish-btn");
  const cancelButton = event.target.closest(".cancel-btn");
  if (!publishButton && !cancelButton) return;
  const planID = (publishButton || cancelButton).dataset.plan;
  if (cancelButton && !window.confirm("取消该计划？已发布的信号将被补偿回滚。")) return;
  const path = publishButton
    ? `/api/plans/${planID}/signals`
    : `/api/plans/${planID}/cancel`;
  await withBusy(publishButton || cancelButton, async () => {
    try {
      const result = await api(path, { method: "POST" });
      if (result.progress && result.progress.status !== "completed") {
        setMessage(message, `发布未完成：${result.progress.error || result.progress.status}，已自动补偿。`);
      } else if (publishButton) {
        setMessage(message, `计划 ${shortID(planID)} 信号已发布。`);
      } else {
        setMessage(message, `计划 ${shortID(planID)} 已取消，补偿 ${result.compensated ?? 0} 条信号。`);
      }
      await refresh();
    } catch (error) {
      setMessage(message, error.message);
      await refresh().catch(() => {});
    }
  });
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = form.querySelector("button");
  await withBusy(button, async () => {
    try {
      const result = await api("/api/plans", { method: "POST", body: JSON.stringify({
        chamber_id: document.querySelector("#chamber").value,
        direction: document.querySelector("#direction").value,
        vessels: [{ name: document.querySelector("#vessel").value, call_sign: document.querySelector("#call-sign").value, length_m: Number(document.querySelector("#length").value) }],
      }) });
      setMessage(message, `Plan ${shortID(result.plan.id)} admitted to ${result.plan.chamber_id}.`);
      await refresh();
    } catch (error) { setMessage(message, error.message); }
  });
});

refreshLoop();
