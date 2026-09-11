use std::collections::{BTreeMap, BTreeSet};
use workflow_core::{ActionDescriptor, SideEffect};

type ActionDefinition<'a> = (
    &'a str,
    &'a str,
    &'a str,
    &'a str,
    &'a [&'a str],
    SideEffect,
);

#[derive(Debug, Clone, Default)]
pub struct ActionRegistry {
    actions: BTreeMap<String, ActionDescriptor>,
}

impl ActionRegistry {
    pub fn built_in() -> Self {
        let mut registry = Self::default();
        let definitions: &[ActionDefinition<'_>] = &[
            (
                "browser.launch",
                "启动浏览器",
                "启动新的 Chrome 或 Chromium 浏览器会话。",
                "浏览器",
                &["browser.launch"],
                SideEffect::BrowserState,
            ),
            (
                "browser.close",
                "关闭浏览器",
                "关闭当前运行拥有的浏览器会话。",
                "浏览器",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "browser.setCookie",
                "设置 Cookie",
                "向当前浏览器会话写入指定 Cookie。",
                "浏览器",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "browser.getCookies",
                "获取 Cookies",
                "获取当前页面或指定域名的全部 Cookie列表。",
                "浏览器",
                &["browser.interact"],
                SideEffect::Pure,
            ),
            (
                "browser.injectStealth",
                "反检测特征注入",
                "向当前页面注入隐蔽防爬虫检测与浏览器指纹伪装脚本。",
                "浏览器",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "page.goto",
                "打开页面",
                "导航到指定网址并返回页面标题和最终地址。",
                "页面",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "page.reload",
                "刷新页面",
                "重新加载当前页面。",
                "页面",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "page.back",
                "后退页面",
                "返回浏览历史中的上一页。",
                "页面",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "page.forward",
                "前进页面",
                "前往浏览历史中的下一页。",
                "页面",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "page.wait",
                "等待页面元素",
                "等待指定元素出现在页面中。",
                "页面",
                &["browser.interact"],
                SideEffect::Pure,
            ),
            (
                "page.screenshot",
                "页面截图",
                "截取当前页面并保存为运行产物。",
                "页面",
                &["browser.interact", "file.artifact.write"],
                SideEffect::Idempotent,
            ),
            (
                "page.tab.new",
                "新建标签页",
                "打开新的空白或指定网址的浏览器标签页并切换至该页。",
                "页面",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "page.tab.switch",
                "切换标签页",
                "按标签索引或标题匹配切换活跃标签页。",
                "页面",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "page.tab.close",
                "关闭标签页",
                "关闭当前标签页并切换到前一个标签页。",
                "页面",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "network.waitForResponse",
                "等待网络响应",
                "监听并等待匹配特定网址的请求响应并提取 Body 内容。",
                "网络",
                &["browser.interact"],
                SideEffect::Pure,
            ),
            (
                "download.click",
                "点击下载",
                "点击定位器匹配的下载按钮或链接，并将文件作为运行产物保存。",
                "下载",
                &["browser.interact", "download.create", "file.artifact.write"],
                SideEffect::ExternalWrite,
            ),
            (
                "element.find",
                "查找元素",
                "检查定位器是否匹配元素，并返回匹配数量。",
                "元素",
                &["browser.interact"],
                SideEffect::Pure,
            ),
            (
                "element.click",
                "点击元素",
                "点击定位器匹配的页面元素。",
                "元素",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "element.input",
                "输入文本",
                "向输入框或文本区域输入内容。",
                "元素",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "element.clear",
                "清空输入框",
                "清除输入框或文本区域中的现有内容。",
                "元素",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "element.select",
                "选择下拉选项",
                "按文本或值选择下拉框选项。",
                "元素",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "element.hover",
                "悬停元素",
                "把鼠标移动到指定元素上方。",
                "元素",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "element.scrollIntoView",
                "滚动到元素",
                "滚动页面，使目标元素进入可视区域。",
                "元素",
                &["browser.interact"],
                SideEffect::BrowserState,
            ),
            (
                "element.extract",
                "提取元素内容",
                "提取元素文本、属性、HTML 或输入值。",
                "元素",
                &["browser.interact"],
                SideEffect::Pure,
            ),
            (
                "element.extractAll",
                "批量提取元素",
                "从多个匹配元素中提取结构化记录。",
                "元素",
                &["browser.interact"],
                SideEffect::Pure,
            ),
            (
                "data.set",
                "设置数据",
                "保存一个值供后续步骤引用。",
                "数据",
                &[],
                SideEffect::Pure,
            ),
            (
                "data.wait",
                "等待一段时间",
                "暂停指定时长，常用于调试或节流。",
                "数据",
                &[],
                SideEffect::Pure,
            ),
            (
                "data.randomWait",
                "随机等待",
                "在指定的最小和最大时长区间内随机等待，模拟真实人类操作间隔与反爬风控对抗。",
                "数据",
                &[],
                SideEffect::Pure,
            ),
            (
                "data.filter",
                "过滤数据",
                "按受限表达式过滤数组记录。",
                "数据",
                &[],
                SideEffect::Pure,
            ),
            (
                "data.deduplicate",
                "数据去重",
                "按一个或多个字段保留首次出现的记录。",
                "数据",
                &[],
                SideEffect::Pure,
            ),
            (
                "data.uniqueBy",
                "按字段去重",
                "按指定字段保留首次出现的记录。",
                "数据",
                &[],
                SideEffect::Pure,
            ),
            (
                "data.merge",
                "合并数据",
                "合并对象或拼接数组。",
                "数据",
                &[],
                SideEffect::Pure,
            ),
            (
                "ai.extract",
                "AI 智能提取",
                "使用大模型对非结构化文本或网页 HTML 进行结构化抽取。",
                "AI",
                &["network.public"],
                SideEffect::Pure,
            ),
            (
                "ai.vision",
                "AI 视觉分析",
                "捕获网页视图并使用多模态视觉大模型判定页面状态或内容。",
                "AI",
                &["browser.interact", "network.public"],
                SideEffect::Pure,
            ),
            (
                "plugin.element.smartScrape",
                "智能元素抓取插件",
                "高阶页面元素抽取插件：支持文本/属性/HTML/正则混合提取与去重。",
                "插件",
                &["browser.interact"],
                SideEffect::Pure,
            ),
            (
                "assert.equal",
                "断言相等",
                "检查实际值与期望值是否相等。",
                "断言",
                &[],
                SideEffect::Pure,
            ),
            (
                "assert.match",
                "断言匹配",
                "检查字符串是否匹配正则表达式。",
                "断言",
                &[],
                SideEffect::Pure,
            ),
            (
                "assert.fail",
                "主动失败",
                "使用指定错误码和消息终止当前流程。",
                "断言",
                &[],
                SideEffect::Pure,
            ),
            (
                "file.writeJson",
                "写入 JSON",
                "把数据写入运行产物目录中的 JSON 文件。",
                "文件",
                &["file.artifact.write"],
                SideEffect::ExternalWrite,
            ),
            (
                "file.summarizeDownloads",
                "汇总下载结果",
                "扫描 downloads 下各期刊文件夹，统计抽出的文章数和成功保存的 PDF 数。",
                "文件",
                &["file.artifact.write"],
                SideEffect::ExternalWrite,
            ),
            (
                "file.writeJsonl",
                "写入 JSONL",
                "把记录逐行写入 JSONL 产物文件。",
                "文件",
                &["file.artifact.write"],
                SideEffect::ExternalWrite,
            ),
            (
                "file.writeCsv",
                "写入 CSV",
                "把结构化记录写入 CSV 产物文件。",
                "文件",
                &["file.artifact.write"],
                SideEffect::ExternalWrite,
            ),
            (
                "notify.feishu",
                "发送飞书通知",
                "向飞书群机器人发送自定义文本或富文本消息。",
                "通知",
                &["network.public"],
                SideEffect::ExternalWrite,
            ),
            (
                "notify.wecom",
                "发送企微通知",
                "向企业微信群机器人发送 Markdown 或文本消息。",
                "通知",
                &["network.public"],
                SideEffect::ExternalWrite,
            ),
            (
                "notify.dingtalk",
                "发送钉钉通知",
                "向钉钉自定义机器人发送 Markdown 消息。",
                "通知",
                &["network.public"],
                SideEffect::ExternalWrite,
            ),
            (
                "notify.webhook",
                "通用 Webhook 通知",
                "向指定 HTTP Webhook 发送 JSON 请求 payload。",
                "通知",
                &["network.public"],
                SideEffect::ExternalWrite,
            ),
        ];
        for (name, title, description, category, permissions, side_effect) in definitions {
            registry.register(ActionDescriptor {
                name: (*name).into(),
                title: (*title).into(),
                description: (*description).into(),
                category: (*category).into(),
                version: "1.0.0".into(),
                input_schema: input_schema(name),
                output_schema: serde_json::json!({"type":"object"}),
                ui_schema: ui_schema(name),
                permissions: permissions.iter().map(|v| (*v).to_owned()).collect(),
                side_effect: *side_effect,
                default_timeout_ms: default_timeout_ms(name),
                sensitive_paths: vec![],
            });
        }
        registry
    }
    pub fn register(&mut self, descriptor: ActionDescriptor) {
        self.actions.insert(descriptor.name.clone(), descriptor);
    }
    pub fn get(&self, name: &str) -> Option<&ActionDescriptor> {
        self.actions.get(name)
    }
    pub fn list(&self) -> impl Iterator<Item = &ActionDescriptor> {
        self.actions.values()
    }
    pub fn permissions_for<'a>(
        &self,
        names: impl IntoIterator<Item = &'a str>,
    ) -> BTreeSet<String> {
        names
            .into_iter()
            .filter_map(|n| self.get(n))
            .flat_map(|d| d.permissions.iter().cloned())
            .collect()
    }
}

fn input_schema(name: &str) -> serde_json::Value {
    use serde_json::json;
    match name {
        "browser.launch" => {
            json!({"type":"object","properties":{"headless":{"type":"boolean","title":"无头模式","default":false},"profile":{"type":"string","title":"浏览器 Profile","placeholder":"ephemeral"}}})
        }
        "browser.setCookie" => {
            json!({"type":"object","required":["name","value"],"properties":{"name":{"type":"string","title":"Cookie 名称"},"value":{"type":"string","title":"Cookie 值","expression":true},"domain":{"type":"string","title":"作用域名（可选）"},"path":{"type":"string","title":"作用路径","default":"/"}}})
        }
        "browser.getCookies" => {
            json!({"type":"object","properties":{}})
        }
        "browser.injectStealth" => {
            json!({
                "type": "object",
                "properties": {
                    "customScript": {
                        "type": "string",
                        "title": "追加自定义反反爬 JS 代码（可选）",
                        "placeholder": "Object.defineProperty(navigator, 'customVar', { get: () => 1 });"
                    }
                }
            })
        }
        "page.tab.new" => {
            json!({"type":"object","properties":{"url":{"type":"string","format":"uri","title":"初始网址（可选）","placeholder":"https://example.com"}}})
        }
        "page.tab.switch" => {
            json!({"type":"object","properties":{"index":{"type":"integer","title":"标签页索引 (0-based)"},"title":{"type":"string","title":"标签页标题关键字"}}})
        }
        "page.tab.close" => {
            json!({"type":"object","properties":{}})
        }
        "network.waitForResponse" => {
            json!({"type":"object","required":["urlPattern"],"properties":{"urlPattern":{"type":"string","title":"网址关键字或正则","placeholder":"api/v1/articles"},"timeout":{"type":"string","title":"等待超时","default":"15s"}}})
        }
        "page.goto" => {
            json!({"type":"object","required":["url"],"properties":{"url":{"type":"string","format":"uri","title":"打开网址","placeholder":"https://example.com"}}})
        }
        "page.wait" => {
            json!({"type":"object","required":["locator"],"properties":{"locator":locator_schema(),"timeout":{"type":"string","title":"等待超时","placeholder":"15s","default":"15s"}}})
        }
        "page.screenshot" => {
            json!({"type":"object","properties":{"path":{"type":"string","format":"artifact-path","title":"截图文件名","placeholder":"screenshot.png","default":"screenshot.png"}}})
        }
        "download.click" => {
            json!({"type":"object","properties":{"locator":locator_schema(),"url":{"type":"string","title":"直接下载网址","placeholder":"https://example.com/article.pdf"},"filename":{"type":"string","format":"artifact-path","title":"保存文件名（可选）","placeholder":"downloads/article.pdf"},"timeout":{"type":"string","title":"下载超时","placeholder":"60s","default":"60s"}}})
        }
        "element.input" => {
            json!({"type":"object","required":["locator","text"],"properties":{"locator":locator_schema(),"text":{"type":"string","title":"输入内容","expression":true,"placeholder":"文本或 ${inputs.keyword}"}}})
        }
        "element.select" => {
            json!({"type":"object","required":["locator"],"properties":{"locator":locator_schema(),"value":{"type":"string","title":"选项值"},"text":{"type":"string","title":"选项文本"}}})
        }
        "element.extract" => {
            json!({"type":"object","required":["locator"],"properties":{"locator":locator_schema(),"value":{"type":"string","title":"提取内容","enum":["text","html","attribute"],"default":"text"},"attribute":{"type":"string","title":"属性名"}}})
        }
        "element.extractAll" => {
            json!({"type":"object","required":["locator"],"properties":{"locator":locator_schema(),"limit":{"type":"integer","title":"最大数量","minimum":1,"default":100},"fields":{"type":"object","title":"字段定义"}}})
        }
        action if action.starts_with("element.") => {
            json!({"type":"object","required":["locator"],"properties":{"locator":locator_schema()}})
        }
        "data.wait" => {
            json!({"type":"object","required":["duration"],"properties":{"duration":{"type":"string","title":"等待时长","placeholder":"1s","default":"1s"}}})
        }
        "data.randomWait" => {
            json!({
                "type": "object",
                "required": ["minDuration", "maxDuration"],
                "properties": {
                    "minDuration": {
                        "type": "string",
                        "title": "最小等待时长",
                        "placeholder": "1s",
                        "default": "1s"
                    },
                    "maxDuration": {
                        "type": "string",
                        "title": "最大等待时长",
                        "placeholder": "3s",
                        "default": "3s"
                    }
                }
            })
        }
        "data.set" => {
            json!({"type":"object","required":["value"],"properties":{"value":{"title":"数据值","expression":true}}})
        }
        "data.filter" => {
            json!({"type":"object","required":["items","condition"],"properties":{"items":{"type":"array","title":"记录数组","expression":true},"condition":{"type":"string","title":"过滤条件","expression":true,"placeholder":"${item.active == true}"}}})
        }
        "data.deduplicate" => {
            json!({"type":"object","required":["items","keys"],"properties":{"items":{"type":"array","title":"记录数组","expression":true},"keys":{"type":"array","title":"去重字段","placeholder":"[\"doi\",\"url\"]"}}})
        }
        "data.uniqueBy" => {
            json!({"type":"object","required":["items","key"],"properties":{"items":{"type":"array","title":"记录数组","expression":true},"key":{"type":"string","title":"去重字段","placeholder":"doi"}}})
        }
        "data.merge" => {
            json!({"type":"object","required":["values"],"properties":{"values":{"type":"array","title":"待合并值","expression":true}}})
        }
        "ai.extract" => {
            json!({"type":"object","required":["content","prompt"],"properties":{"content":{"type":"string","title":"待提取文本或 HTML","expression":true},"prompt":{"type":"string","title":"抽取指令","placeholder":"提取文章标题、作者列表与发布日期"},"model":{"type":"string","title":"模型名称","default":"gpt-4o-mini"}}})
        }
        "ai.vision" => {
            json!({
                "type": "object",
                "required": ["prompt"],
                "properties": {
                    "prompt": { "type": "string", "title": "视觉分析指令", "placeholder": "判断当前页面是否登录成功，是否有滑块验证码" },
                    "imagePath": { "type": "string", "format": "artifact-path", "title": "截图产物路径（可选，留空自动全屏截图）" }
                }
            })
        }
        "plugin.element.smartScrape" => {
            json!({
                "type": "object",
                "required": ["locator"],
                "properties": {
                    "locator": locator_schema(),
                    "fields": {
                        "type": "object",
                        "title": "提取字段映射",
                        "placeholder": "{\"title\": \"text\", \"link\": \"attr:href\"}"
                    },
                    "limit": {
                        "type": "integer",
                        "title": "最大提取行数",
                        "default": 50,
                        "minimum": 1
                    },
                    "uniqueBy": {
                        "type": "string",
                        "title": "去重字段（可选）",
                        "placeholder": "link"
                    }
                }
            })
        }
        "notify.feishu" => {
            json!({
                "type": "object",
                "required": ["webhookUrl", "text"],
                "properties": {
                    "webhookUrl": { "type": "string", "format": "uri", "title": "飞书 Webhook 地址", "placeholder": "https://open.feishu.cn/open-apis/bot/v2/hook/..." },
                    "title": { "type": "string", "title": "卡片标题（可选）", "placeholder": "任务执行完成" },
                    "text": { "type": "string", "title": "通知正文", "expression": true, "placeholder": "采集已完成，共获取 ${outputs.count} 条记录" }
                }
            })
        }
        "notify.wecom" => {
            json!({
                "type": "object",
                "required": ["webhookUrl", "content"],
                "properties": {
                    "webhookUrl": { "type": "string", "format": "uri", "title": "企业微信 Webhook 地址", "placeholder": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=..." },
                    "content": { "type": "string", "title": "Markdown 正文", "expression": true, "placeholder": "### 任务提醒\n- 状态: 成功" }
                }
            })
        }
        "notify.dingtalk" => {
            json!({
                "type": "object",
                "required": ["webhookUrl", "title", "text"],
                "properties": {
                    "webhookUrl": { "type": "string", "format": "uri", "title": "钉钉 Webhook 地址", "placeholder": "https://oapi.dingtalk.com/robot/send?access_token=..." },
                    "title": { "type": "string", "title": "首屏会话透出标题", "placeholder": "自动化报警" },
                    "text": { "type": "string", "title": "Markdown 正文", "expression": true, "placeholder": "#### 巡检报告\n> 状态: 正常" }
                }
            })
        }
        "notify.webhook" => {
            json!({
                "type": "object",
                "required": ["url"],
                "properties": {
                    "url": { "type": "string", "format": "uri", "title": "目标 URL", "placeholder": "https://api.example.com/callback" },
                    "method": { "type": "string", "title": "请求方法", "enum": ["POST", "PUT", "GET"], "default": "POST" },
                    "headers": { "type": "object", "title": "自定义请求头", "placeholder": "{\"Authorization\": \"Bearer ...\"}" },
                    "data": { "title": "请求 Body", "expression": true }
                }
            })
        }
        "assert.equal" => {
            json!({"type":"object","required":["actual","expected"],"properties":{"actual":{"title":"实际值","expression":true},"expected":{"title":"期望值","expression":true}}})
        }
        "assert.match" => {
            json!({"type":"object","required":["value","pattern"],"properties":{"value":{"type":"string","title":"待匹配文本","expression":true},"pattern":{"type":"string","title":"正则表达式","placeholder":"^https://"}}})
        }
        action if action.starts_with("file.") => {
            json!({"type":"object","required":["path","data"],"properties":{"path":{"type":"string","format":"artifact-path","title":"产物文件名","placeholder":"result.json"},"data":{"title":"写入数据","expression":true}}})
        }
        _ => json!({"type":"object","properties":{}}),
    }
}
fn locator_schema() -> serde_json::Value {
    serde_json::json!({"type":"string","format":"locator","title":"元素定位器","placeholder":"css:button[type=submit]"})
}
fn ui_schema(name: &str) -> serde_json::Value {
    serde_json::json!({"order": input_schema(name).get("properties").and_then(serde_json::Value::as_object).map(|value| value.keys().cloned().collect::<Vec<_>>()).unwrap_or_default()})
}
fn default_timeout_ms(name: &str) -> u64 {
    match name {
        "data.wait" | "data.randomWait" => 60_000,
        "page.goto" => 30_000,
        "download.click" => 60_000,
        "network.waitForResponse" => 30_000,
        _ => 15_000,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn registry_collects_permissions() {
        let r = ActionRegistry::built_in();
        assert_eq!(
            r.permissions_for(["page.goto", "file.writeJson"]),
            BTreeSet::from(["browser.interact".into(), "file.artifact.write".into()])
        );
        assert_eq!(r.get("element.click").unwrap().title, "点击元素");
    }
}
