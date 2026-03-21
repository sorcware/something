import { Tree } from "react-arborist"

function normaliseNodes(nodes, parentPath = "") {
  return nodes.map((node) => {
    const id = parentPath ? `${parentPath}/${node.name}` : node.name
    if (node.children) {
      return { id, name: node.name, children: normaliseNodes(node.children, id) }
    }
    return { id, name: node.name, df_name: node.df_name ?? node.name }
  })
}

function countNodes(nodes) {
  return nodes.reduce(
    (acc, n) => acc + 1 + (n.children ? countNodes(n.children) : 0),
    0
  )
}

export default function TableTree({ tables, onInsert }) {
  const data = normaliseNodes(tables ?? [])
  const nodeCount = countNodes(data)
  const height = Math.min(Math.max(nodeCount * 28, 60), 300)

  return (
    <Tree data={data} height={height} width="100%" openByDefault={false} disableDrag disableDrop disableEdit indent={32}>
      {({ node, style, dragHandle }) => (
        <div
          ref={dragHandle}
          style={{
            ...style,
            display: "flex",
            alignItems: "center",
            gap: "6px",
            paddingRight: "8px",
            fontSize: "13px",
            borderRadius: "4px",
            cursor: "pointer",
          }}
          onMouseEnter={e => e.currentTarget.style.background = "rgba(128,128,128,0.15)"}
          onMouseLeave={e => e.currentTarget.style.background = "transparent"}
          onClick={() => {
            if (!node.isLeaf) {
              node.toggle()
            } else {
              onInsert?.(node.data.df_name)
            }
          }}
          title={node.isLeaf ? "Click to insert into query" : undefined}
        >
          <span>{node.isLeaf ? "📄" : node.isOpen ? "📂" : "📁"}</span>
          <span>{node.data.name}</span>
        </div>
      )}
    </Tree>
  )
}