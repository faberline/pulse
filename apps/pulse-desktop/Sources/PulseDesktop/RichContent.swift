import SwiftUI

enum RichBlock: Equatable {
  case text(String)
  case code(String)
  case table([[String]])
}
func richBlocks(_ source: String) -> [RichBlock] {
  let lines = source.components(separatedBy: "\n")
  var result: [RichBlock] = []
  var buffer: [String] = []
  var code = false
  func flush() {
    if !buffer.isEmpty {
      result.append(
        code ? .code(buffer.joined(separator: "\n")) : .text(buffer.joined(separator: "\n")))
      buffer = []
    }
  }
  var index = 0
  while index < lines.count {
    let line = lines[index]
    if line.hasPrefix("```") {
      flush()
      code.toggle()
      index += 1
      continue
    }
    if !code, index + 1 < lines.count, line.contains("|"),
      lines[index + 1].contains("-"), lines[index + 1].allSatisfy({ "|-: \t".contains($0) })
    {
      flush()
      var rows = [cells(line)]
      var truncated = columnCount(line) > 16
      index += 2
      while index < lines.count && lines[index].contains("|") {
        truncated = truncated || columnCount(lines[index]) > 16
        rows.append(cells(lines[index]))
        index += 1
      }
      result.append(.table(Array(rows.prefix(129))))
      if truncated || rows.count > 129 { result.append(.text("表格顯示已截斷。")) }
      continue
    }
    buffer.append(line)
    index += 1
  }
  flush()
  return result
}
private func cells(_ line: String) -> [String] {
  Array(
    line.trimmingCharacters(in: CharacterSet(charactersIn: "|"))
      .components(separatedBy: "|").prefix(16)
  ).map { $0.trimmingCharacters(in: .whitespaces) }
}
private func columnCount(_ line: String) -> Int {
  line.trimmingCharacters(in: CharacterSet(charactersIn: "|")).components(separatedBy: "|").count
}
struct RichContent: View {
  let source: String
  var body: some View {
    VStack(alignment: .leading, spacing: 10) {
      ForEach(Array(richBlocks(source).enumerated()), id: \.offset) { _, block in
        switch block {
        case .text(let value):
          // Text does not create a WebView or fetch remote image URLs.
          Text(
            (try? AttributedString(
              markdown: value, options: .init(interpretedSyntax: .inlineOnlyPreservingWhitespace)))
              ?? AttributedString(value)
          ).textSelection(.enabled)
        case .code(let value):
          ScrollView(.horizontal) {
            Text(value).font(.system(.body, design: .monospaced)).textSelection(.enabled).padding(
              10)
          }.background(Color.secondary.opacity(0.08), in: RoundedRectangle(cornerRadius: 6))
        case .table(let rows):
          ScrollView(.horizontal) {
            Grid(alignment: .leading, horizontalSpacing: 12, verticalSpacing: 8) {
              ForEach(Array(rows.enumerated()), id: \.offset) { rowIndex, row in
                GridRow {
                  ForEach(Array(row.enumerated()), id: \.offset) { _, cell in
                    Text(cell).fontWeight(rowIndex == 0 ? .semibold : .regular).textSelection(
                      .enabled
                    ).frame(width: 150, alignment: .leading)
                  }
                }
              }
            }.padding(8)
          }
        }
      }
    }
  }
}
