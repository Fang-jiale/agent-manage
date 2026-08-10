// 极简 Prometheus 指标：计数器注册 + 文本格式导出，无第三方依赖。

export class Metrics {
  private counters = new Map<string, number>();
  private helps = new Map<string, string>();

  counter(name: string, help: string): void {
    this.helps.set(name, help);
    this.counters.set(name, 0);
  }

  inc(name: string, delta = 1): void {
    this.counters.set(name, (this.counters.get(name) ?? 0) + delta);
  }

  // gauges: [名称, help, 当前值]，抓取时现算（如连接数直接读 Map.size）
  render(gauges: [string, string, number][]): string {
    const lines: string[] = [];
    for (const [name, help, value] of gauges) {
      lines.push(`# HELP ${name} ${help}`, `# TYPE ${name} gauge`, `${name} ${value}`);
    }
    for (const [name, value] of this.counters) {
      const help = this.helps.get(name) ?? name;
      lines.push(`# HELP ${name} ${help}`, `# TYPE ${name} counter`, `${name} ${value}`);
    }
    return lines.join("\n") + "\n";
  }
}
