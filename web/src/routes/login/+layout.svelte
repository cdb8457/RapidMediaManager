<script lang="ts">
	import { Separator } from '$lib/components/ui/separator/index.js';

	import { PUBLIC_VERSION } from '$env/static/public';
	import { resolve } from '$app/paths';

	let { children } = $props();
</script>

<div class="flex min-h-svh items-center justify-center p-6 md:p-10">
	<div class="flex w-full max-w-sm flex-col gap-8">
		<header class="flex justify-center gap-2 md:justify-start">
			<a class="flex items-center gap-2" href={resolve('/', {})}>
				<div class="flex size-16 items-center justify-center rounded-md text-primary-foreground">
					<img
						alt="Rapid Engine Logo"
						class="size-16 rounded-md object-cover"
						src="/rapid_engine_logo.png"
					/>
				</div>
				<div>
					<h1 class="text-2xl font-bold">Rapid Engine</h1>
					<span class="truncate text-xs">{PUBLIC_VERSION}</span>
				</div>
			</a>
		</header>
		<main class="flex flex-1 items-center justify-center">
			<div class="w-full max-w-[90vw]">
				{@render children()}
			</div>
		</main>
		<div class="flex flex-col items-center justify-center gap-3 text-center">
			<a
				target="_blank"
				class="underline"
				href="https://maximilian-dorninger.gitbook.io/mediamanager/troubleshooting"
			>
				Trouble logging in?
			</a>
			<footer
				class="flex flex-wrap items-center justify-center gap-x-3 gap-y-1 text-sm text-muted-foreground"
			>
				<a target="_blank" class="underline" href="https://github.com/maxdorninger/MediaManager"
					>GitHub</a
				>
				<Separator class="h-4" orientation="vertical" />
				<a target="_blank" class="underline" href="https://github.com/sponsors/maxdorninger"
					>Donate</a
				>
				<Separator class="h-4" orientation="vertical" />
			</footer>
		</div>
	</div>
</div>
