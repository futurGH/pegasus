import { build as viteBuild } from "vite";
import resolve from "@rollup/plugin-node-resolve";
import path from "path";

async function build(entryPoints, { env, output }) {
	const outfile = output;
	const outdir = path.dirname(outfile);
	const splitting = true;

	const isDev = env === "development";

	try {
		const result = await viteBuild({
			build: {
				lib: {
					entry: entryPoints,
					formats: ["es"],
					fileName: "[name]",
				},
				outDir: outdir,
				emptyOutDir: false,
				minify: isDev ? false : true,
				sourcemap: isDev,
				rollupOptions: {
					output: {
						entryFileNames: "[name].js",
					},
					plugins: [resolve()],
				},
			},
			logLevel: "info",
			define: {
				"process.env.NODE_ENV": `"${env}"`,
				__DEV__: `"${isDev}"`,
			},
		});

		entryPoints.forEach((entryPoint) => {
			console.log(
				'Build completed successfully for "' + entryPoint + '"',
			);
		});
		return result;
	} catch (error) {
		console.error("\nBuild failed:", error);
		process.exit(1);
	}
}

function parseArgv(argv) {
	const args = argv.slice(2);
	const result = { _: [] };

	for (let i = 0; i < args.length; i++) {
		const arg = args[i];

		if (arg.startsWith("--")) {
			const longArg = arg.slice(2);
			if (longArg.includes("=")) {
				const [key, value] = longArg.split("=");
				result[key] = parseValue(value);
			} else if (i + 1 < args.length && !args[i + 1].startsWith("-")) {
				result[longArg] = parseValue(args[++i]);
			} else {
				result[longArg] = true;
			}
		} else if (arg.startsWith("-")) {
			const shortArg = arg.slice(1);
			if (shortArg.includes("=")) {
				const [key, value] = shortArg.split("=");
				result[key] = parseValue(value);
			} else if (i + 1 < args.length && !args[i + 1].startsWith("-")) {
				result[shortArg] = parseValue(args[++i]);
			} else {
				for (const char of shortArg) {
					result[char] = true;
				}
			}
		} else {
			result._.push(parseValue(arg));
		}
	}

	return result;
}

function parseValue(value) {
	if (value === "true") return true;
	if (value === "false") return false;
	if (value === "null") return null;
	if (!isNaN(value)) return Number(value);
	return value;
}

function camelCaseKeys(obj) {
	return Object.fromEntries(
		Object.entries(obj).map(([key, value]) => [
			key.replace(/-([a-z])/g, (_, letter) => letter.toUpperCase()),
			value,
		]),
	);
}

const flags = parseArgv(process.argv);
const options = camelCaseKeys(flags);
const entryPoints = options._;

build(entryPoints, options);
