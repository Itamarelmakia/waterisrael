import React, { useMemo, useRef, useState } from "react";
import { Upload, FileSpreadsheet, X, Loader2, ChevronDown, ChevronLeft, Download, FileText } from "lucide-react";

const API_BASE = import.meta.env.VITE_API_BASE || "https://waterisrael-api.onrender.com";


function isExcelFile(file) {
  const validTypes = [
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
  ];
  const validExtensions = [".xlsx", ".xls"];
  const hasValidExtension = validExtensions.some((ext) => file.name?.toLowerCase().endsWith(ext));
  return validTypes.includes(file.type) || hasValidExtension;
}

function normalizeRow(r) {
  return {
    location: r?.location ?? r?.["מיקום הבדיקה"] ?? r?.row ?? "-",
    description: r?.description ?? r?.["תיאור"] ?? r?.details ?? r?.errorType ?? "-",
    status: r?.status ?? r?.["סטטוס"] ?? "Fail",
    notes: r?.notes ?? r?.["הערות"] ?? "",
    user_comment: r?.user_comment ?? r?.["הערת משתמש"] ?? "",
    required_action: r?.required_action ?? r?.["פעולה נדרשת מול התאגיד"] ?? "",
  };
}

async function validateExcel(file) {
  const formData = new FormData();
  formData.append("file", file, file.name || "upload.xlsx");

  const res = await fetch(`${API_BASE}/validate`, { method: "POST", body: formData });
  const text = await res.text();

  if (!res.ok) throw new Error(`API /validate failed: ${res.status} ${text}`);

  let json;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`Non-JSON response from /validate: ${text}`);
  }

  const rows = Array.isArray(json?.summary_rows) ? json.summary_rows : [];
  return rows.map(normalizeRow);
}

async function fetchExecutiveSummary(file) {
  const formData = new FormData();
  formData.append("file", file, file.name || "upload.xlsx");

  const res = await fetch(`${API_BASE}/executive_summary`, { method: "POST", body: formData });
  const text = await res.text();

  if (!res.ok) throw new Error(`API /executive_summary failed: ${res.status} ${text}`);

  let json;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`Non-JSON response from /executive_summary: ${text}`);
  }

  return json?.summaries ?? {};
}

async function downloadValidationExcel(file) {
  const formData = new FormData();
  formData.append("file", file, file.name || "upload.xlsx");

  const res = await fetch(`${API_BASE}/validate_download`, { method: "POST", body: formData });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API /validate_download failed: ${res.status} ${text}`);
  }

  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "validation_output.xlsx";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

export default function App() {
  const [selectedFile, setSelectedFile] = useState(null);
  const [validationResults, setValidationResults] = useState([]);
  const [isDragOver, setIsDragOver] = useState(false);
  const [isValidating, setIsValidating] = useState(false);
  const [hasResults, setHasResults] = useState(false);
  const [expandedRows, setExpandedRows] = useState(new Set());
  const [execSummaries, setExecSummaries] = useState(null);
  const [isLoadingSummary, setIsLoadingSummary] = useState(false);
  const [showSummaryModal, setShowSummaryModal] = useState(false);

  const fileInputRef = useRef(null);

  const counts = useMemo(() => {
    const pass = validationResults.filter((r) => (r.status || "").toLowerCase() === "pass").length;
    const partial = validationResults.filter((r) => (r.status || "").toLowerCase() === "partial fail").length;
    const fail = validationResults.filter((r) => (r.status || "").toLowerCase() === "fail").length;
    return { pass, partial, fail, total: validationResults.length };
  }, [validationResults]);

  const removeFile = () => {
    setSelectedFile(null);
    setValidationResults([]);
    setHasResults(false);
    setExpandedRows(new Set());
    setExecSummaries(null);
    setShowSummaryModal(false);
    if (fileInputRef.current) fileInputRef.current.value = "";
  };

  const handleExecSummary = async () => {
    if (!selectedFile) return;
    setIsLoadingSummary(true);
    try {
      const summaries = await fetchExecutiveSummary(selectedFile);
      setExecSummaries(summaries);
      setShowSummaryModal(true);
    } catch (err) {
      setExecSummaries({ "שגיאה": String(err?.message || err) });
      setShowSummaryModal(true);
    } finally {
      setIsLoadingSummary(false);
    }
  };

  const toggleRowExpansion = (index) => {
    const s = new Set(expandedRows);
    if (s.has(index)) s.delete(index);
    else s.add(index);
    setExpandedRows(s);
  };

  const handleFileSelect = (e) => {
    const file = e.target.files?.[0];
    if (file && isExcelFile(file)) setSelectedFile(file);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setIsDragOver(false);
    const file = e.dataTransfer.files?.[0];
    if (file && isExcelFile(file)) setSelectedFile(file);
  };

  const handleValidate = async () => {
    if (!selectedFile) return;

    setIsValidating(true);
    setHasResults(false);

    try {
      const rows = await validateExcel(selectedFile);
      setValidationResults(rows);
      setHasResults(true);
    } catch (err) {
      setValidationResults([
        {
          location: "שגיאת מערכת",
          description: String(err?.message || err),
          status: "Fail",
          notes: "",
          user_comment: "",
          required_action: "",
        },
      ]);
      setHasResults(true);
    } finally {
      setIsValidating(false);
    }
  };

  const getStatusBadge = (status) => {
    const s = (status || "").toLowerCase();
    if (s === "pass") return <span style={styles.badgePass}>Pass</span>;
    if (s === "partial fail") return <span style={styles.badgePartial}>Partial Fail</span>;
    if (s === "fail") return <span style={styles.badgeFail}>Fail</span>;
    return <span style={styles.badgeNeutral}>{status}</span>;
  };

  return (
    <div dir="rtl" style={styles.page}>
      <div style={styles.container}>
        <div style={{ marginBottom: 24 }}>
          <h1 style={styles.h1}>בדיקת קבצים</h1>
          <p style={styles.p}>העלה קובץ אקסל לבדיקה ואימות</p>
          <p style={styles.smallMuted}>API: {API_BASE}</p>
        </div>

        <div style={styles.card}>
          <div
            onClick={() => fileInputRef.current?.click()}
            onDrop={handleDrop}
            onDragOver={(e) => {
              e.preventDefault();
              setIsDragOver(true);
            }}
            onDragLeave={() => setIsDragOver(false)}
            style={{
              ...styles.dropzone,
              borderColor: isDragOver ? "#60a5fa" : selectedFile ? "#6ee7b7" : "#e5e7eb",
              background: isDragOver ? "rgba(96,165,250,0.08)" : selectedFile ? "rgba(110,231,183,0.08)" : "rgba(0,0,0,0.01)",
            }}
          >
            <input ref={fileInputRef} type="file" accept=".xlsx,.xls" onChange={handleFileSelect} style={{ display: "none" }} />

            {!selectedFile ? (
              <div style={styles.centerCol}>
                <div style={styles.iconBox}>
                  <Upload size={28} />
                </div>
                <div style={{ fontSize: 18, fontWeight: 600, color: "#334155" }}>גרור קובץ לכאן או לחץ לבחירה</div>
                <div style={{ fontSize: 13, color: "#64748b", marginTop: 6 }}>קבצי Excel בלבד (xlsx, xls)</div>
              </div>
            ) : (
              <div style={styles.centerCol}>
                <div style={{ ...styles.iconBox, background: "rgba(16,185,129,0.15)", color: "#059669" }}>
                  <FileSpreadsheet size={28} />
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                  <div style={{ fontSize: 18, fontWeight: 700, color: "#0f172a" }}>{selectedFile.name}</div>
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      removeFile();
                    }}
                    style={styles.iconBtn}
                    title="הסר קובץ"
                  >
                    <X size={16} />
                  </button>
                </div>
                <div style={{ fontSize: 13, color: "#64748b", marginTop: 6 }}>{(selectedFile.size / 1024).toFixed(1)} KB</div>
              </div>
            )}
          </div>
        </div>

        <div style={{ display: "flex", justifyContent: "center", gap: 12, margin: "18px 0 28px" }}>
          <button onClick={handleValidate} disabled={!selectedFile || isValidating} style={{ ...styles.primaryBtn, opacity: !selectedFile || isValidating ? 0.55 : 1 }}>
            {isValidating ? (
              <span style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
                <Loader2 size={18} className="spin" />
                בודק קובץ...
              </span>
            ) : (
              "בדוק קובץ"
            )}
          </button>

          <button
            onClick={() => selectedFile && downloadValidationExcel(selectedFile)}
            disabled={!selectedFile || isValidating}
            style={{ ...styles.secondaryBtn, opacity: !selectedFile || isValidating ? 0.55 : 1 }}
            title="הורד קובץ תוצאות"
          >
            <span style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
              <Download size={18} />
              הורד Excel
            </span>
          </button>

          <button
            onClick={handleExecSummary}
            disabled={!selectedFile || isValidating || isLoadingSummary}
            style={{ ...styles.secondaryBtn, opacity: !selectedFile || isValidating || isLoadingSummary ? 0.55 : 1 }}
            title="תקציר מנהלים"
          >
            <span style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
              {isLoadingSummary ? <Loader2 size={18} className="spin" /> : <FileText size={18} />}
              {isLoadingSummary ? "יוצר תקציר..." : "תקציר מנהלים"}
            </span>
          </button>
        </div>

        {hasResults && (
          <div style={styles.card}>
            <div style={styles.cardHeader}>
              <div>
                <div style={{ fontSize: 18, fontWeight: 800, color: "#0f172a" }}>תוצאות בדיקה</div>
                <div style={{ fontSize: 13, color: "#64748b", marginTop: 4 }}>נמצאו {counts.total} רשומות</div>
              </div>

              <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                <span style={styles.pill}>Pass: {counts.pass}</span>
                <span style={styles.pill}>Partial: {counts.partial}</span>
                <span style={styles.pill}>Fail: {counts.fail}</span>
              </div>
            </div>

            <div style={{ overflowX: "auto" }}>
              <table style={styles.table}>
                <thead>
                  <tr>
                    <th style={styles.th}>מיקום הבדיקה</th>
                    <th style={styles.th}>תיאור</th>
                    <th style={{ ...styles.th, width: 140 }}>סטטוס</th>
                  </tr>
                </thead>
                <tbody>
                  {validationResults.map((r, idx) => {
                    const hasAdvanced = !!(r.notes || r.user_comment || r.required_action);
                    const expanded = expandedRows.has(idx);

                    return (
                      <React.Fragment key={idx}>
                        <tr style={styles.tr}>
                          <td style={styles.td}>
                            <div style={{ display: "flex", gap: 8, alignItems: "flex-start" }}>
                              {hasAdvanced ? (
                                <button onClick={() => toggleRowExpansion(idx)} style={styles.expandBtn} title="הצג פרטים">
                                  {expanded ? <ChevronDown size={16} /> : <ChevronLeft size={16} />}
                                </button>
                              ) : (
                                <span style={{ width: 26 }} />
                              )}
                              <div style={{ whiteSpace: "pre-wrap" }}>{r.location}</div>
                            </div>
                          </td>
                          <td style={styles.td}>
                            <div style={{ whiteSpace: "pre-wrap" }}>{r.description}</div>
                          </td>
                          <td style={styles.td}>{getStatusBadge(r.status)}</td>
                        </tr>

                        {expanded && hasAdvanced && (
                          <tr>
                            <td colSpan={3} style={{ ...styles.td, background: "rgba(148,163,184,0.10)" }}>
                              <div style={{ display: "grid", gap: 10, paddingRight: 26 }}>
                                {r.notes ? (
                                  <div>
                                    <div style={styles.detailTitle}>הערות</div>
                                    <div style={styles.detailBody}>{r.notes}</div>
                                  </div>
                                ) : null}
                                {r.user_comment ? (
                                  <div>
                                    <div style={styles.detailTitle}>הערת משתמש</div>
                                    <div style={styles.detailBody}>{r.user_comment}</div>
                                  </div>
                                ) : null}
                                {r.required_action ? (
                                  <div>
                                    <div style={styles.detailTitle}>פעולה נדרשת מול התאגיד</div>
                                    <div style={styles.detailBody}>{r.required_action}</div>
                                  </div>
                                ) : null}
                              </div>
                            </td>
                          </tr>
                        )}
                      </React.Fragment>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {showSummaryModal && execSummaries && (
          <div style={styles.modalOverlay} onClick={() => setShowSummaryModal(false)}>
            <div style={styles.modalContent} onClick={(e) => e.stopPropagation()}>
              <div style={styles.modalHeader}>
                <div style={{ fontSize: 20, fontWeight: 800, color: "#0f172a" }}>תקציר מנהלים</div>
                <button onClick={() => setShowSummaryModal(false)} style={styles.iconBtn} title="סגור">
                  <X size={18} />
                </button>
              </div>
              <div style={styles.modalBody}>
                {Object.entries(execSummaries).map(([utility, text]) => (
                  <div key={utility} style={{ marginBottom: 24 }}>
                    <div style={{ fontSize: 16, fontWeight: 700, color: "#1e40af", marginBottom: 8, borderBottom: "2px solid #dbeafe", paddingBottom: 6 }}>
                      {utility}
                    </div>
                    <div style={{ whiteSpace: "pre-wrap", fontSize: 14, lineHeight: 1.7, color: "#1e293b" }}>
                      {text}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        <style>{`
          .spin { animation: spin 1s linear infinite; }
          @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
          * { box-sizing: border-box; }
          button { cursor: pointer; }
        `}</style>
      </div>
    </div>
  );
}

const styles = {
  page: {
    minHeight: "100vh",
    background: "linear-gradient(#f8fafc, #ffffff)",
    fontFamily: "system-ui, -apple-system, Segoe UI, Roboto, Arial",
  },
  container: { maxWidth: 980, margin: "0 auto", padding: "40px 18px" },
  h1: { fontSize: 28, margin: 0, color: "#0f172a" },
  p: { margin: "8px 0 0", color: "#475569", fontSize: 16 },
  smallMuted: { margin: "10px 0 0", color: "#94a3b8", fontSize: 12 },
  card: {
    background: "rgba(255,255,255,0.9)",
    border: "1px solid rgba(226,232,240,0.8)",
    borderRadius: 18,
    boxShadow: "0 10px 30px rgba(15, 23, 42, 0.06)",
    padding: 18,
  },
  dropzone: {
    border: "2px dashed #e5e7eb",
    borderRadius: 18,
    padding: 36,
    transition: "all 0.2s ease",
  },
  centerCol: { display: "flex", flexDirection: "column", alignItems: "center", gap: 8 },
  iconBox: {
    width: 58,
    height: 58,
    borderRadius: 16,
    background: "rgba(148,163,184,0.18)",
    color: "#64748b",
    display: "grid",
    placeItems: "center",
    marginBottom: 6,
  },
  iconBtn: {
    border: "none",
    background: "rgba(148,163,184,0.18)",
    color: "#334155",
    borderRadius: 999,
    padding: 6,
    lineHeight: 0,
  },
  primaryBtn: {
    border: "none",
    background: "#0f172a",
    color: "white",
    padding: "14px 22px",
    borderRadius: 14,
    fontSize: 16,
    fontWeight: 700,
    boxShadow: "0 10px 22px rgba(15, 23, 42, 0.18)",
  },
  secondaryBtn: {
    border: "1px solid rgba(148,163,184,0.5)",
    background: "white",
    color: "#0f172a",
    padding: "14px 18px",
    borderRadius: 14,
    fontSize: 16,
    fontWeight: 700,
  },
  cardHeader: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "flex-start",
    gap: 14,
    padding: "8px 6px 16px",
    borderBottom: "1px solid rgba(226,232,240,0.8)",
    marginBottom: 12,
  },
  pill: {
    padding: "6px 10px",
    borderRadius: 999,
    border: "1px solid rgba(148,163,184,0.35)",
    background: "rgba(248,250,252,0.9)",
    color: "#334155",
    fontSize: 13,
    fontWeight: 700,
  },
  table: { width: "100%", borderCollapse: "collapse" },
  th: {
    textAlign: "right",
    fontSize: 13,
    color: "#334155",
    padding: "12px 10px",
    borderBottom: "2px solid rgba(226,232,240,0.9)",
    background: "rgba(255,255,255,0.9)",
    position: "sticky",
    top: 0,
  },
  tr: { borderBottom: "1px solid rgba(226,232,240,0.8)" },
  td: { verticalAlign: "top", padding: "12px 10px", color: "#0f172a", fontSize: 14 },
  expandBtn: {
    border: "none",
    background: "rgba(148,163,184,0.18)",
    borderRadius: 8,
    padding: 4,
    lineHeight: 0,
    color: "#334155",
  },
  badgePass: { display: "inline-flex", padding: "6px 10px", borderRadius: 10, border: "1px solid #a7f3d0", background: "#ecfdf5", color: "#047857", fontWeight: 800, fontSize: 12 },
  badgePartial: { display: "inline-flex", padding: "6px 10px", borderRadius: 10, border: "1px solid #fdba74", background: "#fff7ed", color: "#c2410c", fontWeight: 800, fontSize: 12 },
  badgeFail: { display: "inline-flex", padding: "6px 10px", borderRadius: 10, border: "1px solid #fecaca", background: "#fef2f2", color: "#b91c1c", fontWeight: 800, fontSize: 12 },
  badgeNeutral: { display: "inline-flex", padding: "6px 10px", borderRadius: 10, border: "1px solid rgba(148,163,184,0.35)", background: "rgba(248,250,252,0.9)", color: "#334155", fontWeight: 800, fontSize: 12 },
  detailTitle: { fontSize: 12, fontWeight: 800, color: "#475569", marginBottom: 4 },
  detailBody: { fontSize: 13, color: "#0f172a", whiteSpace: "pre-wrap" },
  modalOverlay: {
    position: "fixed",
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: "rgba(15, 23, 42, 0.5)",
    backdropFilter: "blur(4px)",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    zIndex: 1000,
    padding: 20,
  },
  modalContent: {
    background: "white",
    borderRadius: 20,
    boxShadow: "0 25px 60px rgba(15, 23, 42, 0.25)",
    maxWidth: 760,
    width: "100%",
    maxHeight: "85vh",
    display: "flex",
    flexDirection: "column",
    direction: "rtl",
  },
  modalHeader: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    padding: "18px 22px",
    borderBottom: "1px solid rgba(226,232,240,0.8)",
  },
  modalBody: {
    padding: "22px 26px",
    overflowY: "auto",
    flex: 1,
  },
};
