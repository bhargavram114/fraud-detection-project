import { useState, useEffect, useRef } from "react";

const COLORS = {
  bg: "#0B1120",
  panel: "#111827",
  border: "#1E2D40",
  kafka: "#E84D4D",
  spark: "#F97316",
  sink: "#3B82F6",
  alert: "#FBBF24",
  text: "#E2E8F0",
  muted: "#64748B",
  green: "#22C55E",
  rule: "#A78BFA",
};

const NODES = [
  { id: "gen",     label: "Transaction\nGenerator",   sub: "Faker + kafka-python\n~10 TPS | fraud bursts injected",  x: 20,  y: 42, color: COLORS.green,  icon: "💳" },
  { id: "kafka_in",label: "Kafka Topic\ntransactions", sub: "3 partitions\nretention: 24h",                          x: 230, y: 42, color: COLORS.kafka,  icon: "⚡" },
  { id: "spark",   label: "Spark Structured\nStreaming", sub: "Watermark: 2 min\nShuffle partitions: 4",             x: 460, y: 42, color: COLORS.spark,  icon: "🔥" },
  { id: "rule1",   label: "Rule 1\nVelocity Check",   sub: ">5 txns in 5-min window\nOR total >₹50,000",            x: 460, y: 200, color: COLORS.rule,  icon: "📊" },
  { id: "rule2",   label: "Rule 2\nHigh Value",       sub: "Single txn >₹30,000\nRow-level filter",                 x: 680, y: 200, color: COLORS.rule,  icon: "🚨" },
  { id: "merge",   label: "Alert\nMerger",            sub: "union() all rules\nAdd new rules here",                  x: 570, y: 300, color: COLORS.alert, icon: "🔀" },
  { id: "kafka_out",label:"Kafka Topic\nfraud-alerts", sub: "Real-time consumers\nNotification / Case Mgmt",         x: 350, y: 400, color: COLORS.kafka, icon: "📢" },
  { id: "parquet", label: "Parquet\nData Lake",       sub: "outputMode: append\nPartitioned by date",                x: 570, y: 400, color: COLORS.sink,  icon: "🗄️" },
  { id: "console", label: "Console\nMonitor",         sub: "Dev/debug sink\noutputMode: update",                     x: 790, y: 400, color: COLORS.muted, icon: "🖥️" },
];

const EDGES = [
  { from: "gen",     to: "kafka_in",  label: "JSON events" },
  { from: "kafka_in",to: "spark",     label: "readStream" },
  { from: "spark",   to: "rule1",     label: "" },
  { from: "spark",   to: "rule2",     label: "" },
  { from: "rule1",   to: "merge",     label: "" },
  { from: "rule2",   to: "merge",     label: "" },
  { from: "merge",   to: "kafka_out", label: "writeStream" },
  { from: "merge",   to: "parquet",   label: "writeStream" },
  { from: "merge",   to: "console",   label: "writeStream" },
];

const CONCEPTS = [
  {
    title: "Watermarking",
    color: COLORS.spark,
    body: "withWatermark(\"event_time\", \"2 min\") tells Spark how late an event can arrive before its window is finalised and dropped from state. Without it, Spark keeps ALL window state in memory forever → OOM.",
    code: ".withWatermark(\"event_time\", \"2 minutes\")\n.groupBy(window(...), col(\"card_id\"))",
  },
  {
    title: "Sliding vs Tumbling Windows",
    color: COLORS.rule,
    body: "Tumbling = non-overlapping. A fraud burst spanning a boundary escapes. Sliding window(\"5 min\", \"1 min slide\") means each txn falls in up to 5 windows — burst is always caught.",
    code: "window(col(\"event_time\"), \"5 minutes\", \"1 minute\")",
  },
  {
    title: "Output Modes",
    color: COLORS.sink,
    body: "append → only finalised rows (file sinks). update → every changed row (Kafka, console). complete → full table every trigger (tiny aggregations only).",
    code: ".writeStream.outputMode(\"update\")  # Kafka\n.writeStream.outputMode(\"append\")  # Parquet",
  },
  {
    title: "Exactly-Once",
    color: COLORS.alert,
    body: "Checkpoint dir stores Kafka offsets + operator state. On restart, Spark replays from the last committed offset. Kafka's idempotent producer prevents duplicate writes → end-to-end exactly-once.",
    code: ".option(\"checkpointLocation\", \"/tmp/checkpoint\")",
  },
  {
    title: "Shuffle Partitions",
    color: COLORS.green,
    body: "Default is 200 — catastrophic for streaming (200 small tasks per micro-batch). Set to match your Kafka partition count (3) or CPU cores. Massively reduces overhead.",
    code: ".config(\"spark.sql.shuffle.partitions\", \"4\")",
  },
];

function NodeBox({ node, active, onClick }) {
  const lines = node.label.split("\n");
  const subLines = node.sub.split("\n");
  return (
    <div
      onClick={() => onClick(node.id)}
      style={{
        position: "absolute",
        left: node.x,
        top: node.y,
        width: 160,
        padding: "10px 12px",
        background: active ? `${node.color}22` : COLORS.panel,
        border: `1.5px solid ${active ? node.color : COLORS.border}`,
        borderRadius: 10,
        cursor: "pointer",
        transition: "all 0.2s",
        boxShadow: active ? `0 0 18px ${node.color}44` : "none",
        zIndex: 2,
      }}
    >
      <div style={{ fontSize: 18, marginBottom: 4 }}>{node.icon}</div>
      {lines.map((l, i) => (
        <div key={i} style={{ color: node.color, fontWeight: 700, fontSize: 11, fontFamily: "monospace", lineHeight: 1.3 }}>{l}</div>
      ))}
      <div style={{ marginTop: 5, borderTop: `1px solid ${COLORS.border}`, paddingTop: 4 }}>
        {subLines.map((l, i) => (
          <div key={i} style={{ color: COLORS.muted, fontSize: 9, fontFamily: "monospace", lineHeight: 1.5 }}>{l}</div>
        ))}
      </div>
    </div>
  );
}

function SVGArrows({ activeNode }) {
  const nodeCenter = (node) => ({ x: node.x + 80, y: node.y + 45 });

  return (
    <svg style={{ position: "absolute", top: 0, left: 0, width: "100%", height: "100%", pointerEvents: "none", zIndex: 1 }}>
      <defs>
        <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
          <path d="M0,0 L0,6 L8,3 z" fill={COLORS.muted} />
        </marker>
        <marker id="arrow-active" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
          <path d="M0,0 L0,6 L8,3 z" fill={COLORS.alert} />
        </marker>
      </defs>
      {EDGES.map((edge, i) => {
        const from = NODES.find(n => n.id === edge.from);
        const to   = NODES.find(n => n.id === edge.to);
        if (!from || !to) return null;
        const f = nodeCenter(from);
        const t = nodeCenter(to);
        const isActive = activeNode === edge.from || activeNode === edge.to;
        const mx = (f.x + t.x) / 2;
        const my = (f.y + t.y) / 2;
        return (
          <g key={i}>
            <line
              x1={f.x} y1={f.y} x2={t.x} y2={t.y}
              stroke={isActive ? COLORS.alert : COLORS.border}
              strokeWidth={isActive ? 2 : 1.2}
              markerEnd={isActive ? "url(#arrow-active)" : "url(#arrow)"}
              strokeDasharray={isActive ? "none" : "4,3"}
            />
            {edge.label && (
              <text x={mx} y={my - 5} fill={COLORS.muted} fontSize={9} textAnchor="middle" fontFamily="monospace">
                {edge.label}
              </text>
            )}
          </g>
        );
      })}
    </svg>
  );
}

function Particle({ x1, y1, x2, y2, delay }) {
  const [pos, setPos] = useState(0);
  useEffect(() => {
    const timer = setTimeout(() => {
      const interval = setInterval(() => {
        setPos(p => (p >= 1 ? 0 : p + 0.015));
      }, 30);
      return () => clearInterval(interval);
    }, delay);
    return () => clearTimeout(timer);
  }, [delay]);
  const px = x1 + (x2 - x1) * pos;
  const py = y1 + (y2 - y1) * pos;
  return (
    <div style={{
      position: "absolute", left: px - 3, top: py - 3,
      width: 6, height: 6, borderRadius: "50%",
      background: COLORS.green, opacity: 0.8, pointerEvents: "none", zIndex: 3,
      boxShadow: `0 0 6px ${COLORS.green}`,
    }} />
  );
}

export default function FraudPipelineArchitecture() {
  const [activeNode, setActiveNode] = useState(null);
  const [activeConceptIdx, setActiveConceptIdx] = useState(0);
  const [tab, setTab] = useState("diagram");

  const handleNodeClick = (id) => setActiveNode(prev => prev === id ? null : id);

  const activeNodeData = NODES.find(n => n.id === activeNode);

  // Animated particles on the gen → kafka_in edge
  const particles = [0, 800, 1600].map((delay, i) => (
    <Particle key={i} x1={100} y1={62} x2={230} y2={62} delay={delay} />
  ));

  return (
    <div style={{ background: COLORS.bg, minHeight: "100vh", fontFamily: "system-ui, sans-serif", color: COLORS.text, padding: 0 }}>
      {/* Header */}
      <div style={{ padding: "20px 24px 0", borderBottom: `1px solid ${COLORS.border}` }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
          <span style={{ fontSize: 22 }}>🔥</span>
          <span style={{ fontWeight: 800, fontSize: 18, letterSpacing: "-0.5px" }}>ATM Fraud Detection Pipeline</span>
          <span style={{ background: `${COLORS.green}22`, color: COLORS.green, border: `1px solid ${COLORS.green}44`, borderRadius: 99, padding: "2px 10px", fontSize: 11, fontWeight: 700 }}>LIVE STREAM</span>
        </div>
        <div style={{ display: "flex", gap: 0, marginTop: 12 }}>
          {["diagram", "concepts", "flow"].map(t => (
            <button key={t} onClick={() => setTab(t)} style={{
              padding: "6px 18px", fontSize: 12, fontWeight: 600, textTransform: "uppercase",
              background: tab === t ? `${COLORS.spark}22` : "transparent",
              color: tab === t ? COLORS.spark : COLORS.muted,
              border: "none", borderBottom: tab === t ? `2px solid ${COLORS.spark}` : "2px solid transparent",
              cursor: "pointer", letterSpacing: 1,
            }}>{t}</button>
          ))}
        </div>
      </div>

      {/* Diagram Tab */}
      {tab === "diagram" && (
        <div style={{ padding: 24 }}>
          <div style={{ fontSize: 11, color: COLORS.muted, marginBottom: 16 }}>
            Click any node to highlight its connections
          </div>
          <div style={{ position: "relative", height: 520, background: COLORS.panel, borderRadius: 14, border: `1px solid ${COLORS.border}`, overflow: "hidden" }}>
            <SVGArrows activeNode={activeNode} />
            {particles}
            {NODES.map(node => (
              <NodeBox key={node.id} node={node} active={activeNode === node.id} onClick={handleNodeClick} />
            ))}
            {/* Active node tooltip */}
            {activeNodeData && (
              <div style={{
                position: "absolute", bottom: 16, left: 16, right: 16,
                background: `${activeNodeData.color}15`, border: `1px solid ${activeNodeData.color}44`,
                borderRadius: 10, padding: "10px 14px",
              }}>
                <span style={{ color: activeNodeData.color, fontWeight: 700, fontFamily: "monospace", fontSize: 12 }}>
                  {activeNodeData.icon} {activeNodeData.label.replace("\n", " ")}
                </span>
                <div style={{ color: COLORS.muted, fontSize: 11, marginTop: 4, fontFamily: "monospace" }}>
                  {activeNodeData.sub}
                </div>
              </div>
            )}
          </div>
          {/* Legend */}
          <div style={{ display: "flex", gap: 20, marginTop: 14, flexWrap: "wrap" }}>
            {[["Event source", COLORS.green], ["Kafka", COLORS.kafka], ["Spark", COLORS.spark], ["Fraud rules", COLORS.rule], ["Sinks", COLORS.sink], ["Alerts", COLORS.alert]].map(([label, color]) => (
              <div key={label} style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <div style={{ width: 10, height: 10, borderRadius: 2, background: color }} />
                <span style={{ fontSize: 11, color: COLORS.muted }}>{label}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Concepts Tab */}
      {tab === "concepts" && (
        <div style={{ display: "flex", height: "calc(100vh - 100px)" }}>
          <div style={{ width: 200, borderRight: `1px solid ${COLORS.border}`, padding: "16px 0" }}>
            {CONCEPTS.map((c, i) => (
              <button key={i} onClick={() => setActiveConceptIdx(i)} style={{
                display: "block", width: "100%", textAlign: "left",
                padding: "10px 16px", background: activeConceptIdx === i ? `${c.color}18` : "transparent",
                color: activeConceptIdx === i ? c.color : COLORS.muted,
                border: "none", borderLeft: activeConceptIdx === i ? `3px solid ${c.color}` : "3px solid transparent",
                cursor: "pointer", fontSize: 12, fontWeight: activeConceptIdx === i ? 700 : 400,
              }}>{c.title}</button>
            ))}
          </div>
          <div style={{ flex: 1, padding: 28 }}>
            {(() => {
              const c = CONCEPTS[activeConceptIdx];
              return (
                <>
                  <div style={{ color: c.color, fontWeight: 800, fontSize: 16, marginBottom: 14 }}>{c.title}</div>
                  <div style={{ color: COLORS.text, fontSize: 13, lineHeight: 1.7, marginBottom: 20 }}>{c.body}</div>
                  <div style={{
                    background: "#0D1117", border: `1px solid ${COLORS.border}`,
                    borderRadius: 8, padding: 16,
                  }}>
                    <div style={{ color: COLORS.muted, fontSize: 10, marginBottom: 8, letterSpacing: 1 }}>CODE</div>
                    <pre style={{ color: COLORS.green, fontFamily: "monospace", fontSize: 12, margin: 0, whiteSpace: "pre-wrap" }}>
                      {c.code}
                    </pre>
                  </div>
                  <div style={{ marginTop: 20, padding: "12px 16px", background: `${COLORS.alert}15`, borderRadius: 8, border: `1px solid ${COLORS.alert}33` }}>
                    <span style={{ color: COLORS.alert, fontSize: 11, fontWeight: 700 }}>💡 INTERVIEW TIP </span>
                    <span style={{ color: COLORS.muted, fontSize: 11 }}>
                      {i === 0 && "Interviewers often ask: 'What happens without watermarking?' → unbounded state growth → OOM. Be ready to draw the timeline."}
                      {i === 1 && "Draw a 5-minute timeline. Show a burst split across two tumbling windows. Then show how sliding catches it. Visual answers win."}
                      {i === 2 && "Knowing why 'complete' is too expensive is what separates mid-level from senior candidates."}
                      {i === 3 && "Tie this back to ATM ISO exactly-once: if the switch times out and retries, only one debit should post. Same guarantee, different layer."}
                      {i === 4 && "The 200-default shuffle partition question comes up often. Show you know the practical tuning, not just the theory."}
                    </span>
                  </div>
                </>
              );
            })()}
          </div>
        </div>
      )}

      {/* Flow Tab */}
      {tab === "flow" && (
        <div style={{ padding: 24 }}>
          <div style={{ fontSize: 13, color: COLORS.muted, marginBottom: 20 }}>Step-by-step data journey through the pipeline</div>
          {[
            { step: "01", title: "Transaction Generator → Kafka", color: COLORS.green, detail: "Faker generates realistic ATM transactions. Fraud bursts inject 8 transactions from one card in 30 seconds. kafka-python serialises each event as UTF-8 JSON and sends with acks='all' (strongest durability).", file: "producer/transaction_generator.py" },
            { step: "02", title: "Kafka Source (readStream)", color: COLORS.kafka, detail: "Spark reads from the 'transactions' topic with maxOffsetsPerTrigger=10,000 for backpressure. Raw bytes are cast to string, then parsed against TRANSACTION_SCHEMA. Null txn_id rows (failed JSON) are dropped.", file: "jobs/fraud_detection.py → read_kafka_stream()" },
            { step: "03", title: "Watermarking + Window Aggregation", color: COLORS.spark, detail: "Watermark of 2 minutes marks the threshold for late data. Sliding windows of 5 min / 1 min slide group events by (window, card_id). The state store holds incomplete windows in memory until the watermark finalises them.", file: "jobs/fraud_detection.py → detect_velocity_fraud()" },
            { step: "04", title: "Fraud Rules Applied", color: COLORS.rule, detail: "Rule 1 (Velocity): filter aggregated windows where txn_count > 5 OR total_amount > ₹50k. Rule 2 (High Value): row-level filter where single amount > ₹30k. Both emit the same alert schema for easy merging.", file: "jobs/fraud_detection.py → detect_velocity_fraud(), detect_high_value()" },
            { step: "05", title: "Fan-out to 3 Sinks", color: COLORS.sink, detail: "Kafka alerts topic → real-time consumers (notification systems). Parquet data lake → audit trail, batch analytics, ML training data. Console → development monitoring. Each has its own checkpoint directory.", file: "jobs/fraud_detection.py → write_to_*() functions" },
          ].map((step) => (
            <div key={step.step} style={{ display: "flex", gap: 16, marginBottom: 20 }}>
              <div style={{ width: 36, height: 36, borderRadius: "50%", background: `${step.color}22`, border: `2px solid ${step.color}`, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0, fontSize: 11, fontWeight: 800, color: step.color, fontFamily: "monospace" }}>
                {step.step}
              </div>
              <div style={{ background: COLORS.panel, border: `1px solid ${COLORS.border}`, borderRadius: 10, padding: "12px 16px", flex: 1 }}>
                <div style={{ color: step.color, fontWeight: 700, fontSize: 13, marginBottom: 6 }}>{step.title}</div>
                <div style={{ color: COLORS.text, fontSize: 12, lineHeight: 1.6, marginBottom: 8 }}>{step.detail}</div>
                <div style={{ color: COLORS.muted, fontSize: 10, fontFamily: "monospace" }}>📄 {step.file}</div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
