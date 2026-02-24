<script lang="ts">
	import { Separator } from '$lib/components/ui/separator/index.js';
	import * as Sidebar from '$lib/components/ui/sidebar/index.js';
	import * as Breadcrumb from '$lib/components/ui/breadcrumb/index.js';
	import EngineHealthWidget from '$lib/components/engine-health-widget.svelte';
	import {
		Card,
		CardContent,
		CardDescription,
		CardHeader,
		CardTitle
	} from '$lib/components/ui/card';
	import {
		Table,
		TableBody,
		TableCell,
		TableHead,
		TableHeader,
		TableRow
	} from '$lib/components/ui/table';
	import { Badge } from '$lib/components/ui/badge';
	import { Activity, Webhook, DownloadCloud } from 'lucide-svelte';
	import { resolve } from '$app/paths';
	import { onMount } from 'svelte';
	import client from '$lib/api';
	import type { PageProps } from './$types';
	let { data }: PageProps = $props();

	let webhooks: any[] = $state([]);
	let loadingWebhooks = $state(true);

	let activeTransfers: any[] = $state([]);
	let loadingTransfers = $state(true);

	onMount(async () => {
		try {
			// @ts-ignore: Endpoint not in generated OpenAPI spec yet
			const res = await client.GET('/api/v1/engine/webhooks');
			if (res.data) {
				webhooks = res.data as any[];
			}
		} catch (e) {
			console.error('Failed to fetch webhooks', e);
		} finally {
			loadingWebhooks = false;
		}

		try {
			// @ts-ignore: Endpoint not in generated OpenAPI spec yet
			const res = await client.GET('/api/v1/engine/transfers');
			if (res.data) {
				activeTransfers = res.data as any[];
			}
		} catch (e) {
			console.error('Failed to fetch transfers', e);
		} finally {
			loadingTransfers = false;
		}
	});
</script>

<svelte:head>
	<title>Dashboard - MediaManager</title>
	<meta
		content="MediaManager Dashboard - View your recommended movies and TV shows"
		name="description"
	/>
</svelte:head>

<header class="flex h-16 shrink-0 items-center gap-2">
	<div class="flex items-center gap-2 px-4">
		<Sidebar.Trigger class="-ml-1" />
		<Separator class="mr-2 h-4" orientation="vertical" />
		<Breadcrumb.Root>
			<Breadcrumb.List>
				<Breadcrumb.Item class="hidden md:block">
					<Breadcrumb.Link href={resolve('/dashboard', {})}>MediaManager</Breadcrumb.Link>
				</Breadcrumb.Item>
				<Breadcrumb.Separator class="hidden md:block" />
				<Breadcrumb.Item>
					<Breadcrumb.Page>Home</Breadcrumb.Page>
				</Breadcrumb.Item>
			</Breadcrumb.List>
		</Breadcrumb.Root>
	</div>
</header>
<div class="flex flex-1 flex-col gap-4 p-4 pt-0">
	<h1 class="mb-4 scroll-m-20 text-center text-4xl font-extrabold tracking-tight lg:text-5xl">
		Rapid Media Engine
	</h1>
	<main class="min-h-screen flex-1 rounded-xl p-4 md:min-h-min">
		<div class="mx-auto max-w-7xl space-y-8">
			<!-- Mission Control -->
			<div>
				<h2 class="mb-4 flex items-center gap-2 text-2xl font-bold tracking-tight">
					<Activity class="h-6 w-6 text-primary" />
					Mission Control
				</h2>
				<EngineHealthWidget />
			</div>

			<div class="mb-8 grid gap-4 md:grid-cols-2 lg:grid-cols-2">
				<!-- Seerr Webhooks Log -->
				<Card class="flex h-full flex-col">
					<CardHeader>
						<div class="flex items-center gap-2">
							<Webhook class="h-5 w-5 text-muted-foreground" />
							<CardTitle>Recent Webhooks</CardTitle>
						</div>
						<CardDescription>Latest payload signals injected by Seerr.</CardDescription>
					</CardHeader>
					<CardContent class="flex-1">
						<Table>
							<TableHeader>
								<TableRow>
									<TableHead class="w-[100px]">Status</TableHead>
									<TableHead>Event</TableHead>
									<TableHead>Media</TableHead>
									<TableHead class="text-right">Time</TableHead>
								</TableRow>
							</TableHeader>
							<TableBody>
								{#if loadingWebhooks}
									<TableRow>
										<TableCell colspan={4} class="h-24 text-center text-muted-foreground">
											Loading...
										</TableCell>
									</TableRow>
								{:else if webhooks.length === 0}
									<TableRow>
										<TableCell colspan={4} class="h-24 text-center text-muted-foreground">
											No webhooks received yet.
										</TableCell>
									</TableRow>
								{:else}
									{#each webhooks as webhook}
										<TableRow>
											<TableCell>
												{#if webhook.status === 'success'}
													<Badge variant="default" class="bg-green-600 hover:bg-green-700"
														>Success</Badge
													>
												{:else if webhook.status === 'ignored'}
													<Badge variant="secondary">Ignored</Badge>
												{:else}
													<Badge variant="destructive">Error</Badge>
												{/if}
											</TableCell>
											<TableCell class="text-xs font-medium">{webhook.event}</TableCell>
											<TableCell class="font-medium">{webhook.media_title}</TableCell>
											<TableCell class="text-right text-xs text-muted-foreground">
												{new Date(webhook.timestamp).toLocaleTimeString([], {
													hour: '2-digit',
													minute: '2-digit'
												})}
											</TableCell>
										</TableRow>
									{/each}
								{/if}
							</TableBody>
						</Table>
					</CardContent>
				</Card>

				<!-- Active Debrid Transfers -->
				<Card class="flex h-full flex-col">
					<CardHeader>
						<div class="flex items-center gap-2">
							<DownloadCloud class="h-5 w-5 text-muted-foreground" />
							<CardTitle>Active Debrid Transfers</CardTitle>
						</div>
						<CardDescription>Live caching progress on Real-Debrid network.</CardDescription>
					</CardHeader>
					<CardContent class="flex-1">
						<Table>
							<TableHeader>
								<TableRow>
									<TableHead class="w-[100px]">Hash</TableHead>
									<TableHead>Title</TableHead>
									<TableHead>Progress</TableHead>
									<TableHead class="text-right">Speed</TableHead>
								</TableRow>
							</TableHeader>
							<TableBody>
								{#if loadingTransfers}
									<TableRow>
										<TableCell colspan={4} class="h-24 text-center text-muted-foreground">
											Loading...
										</TableCell>
									</TableRow>
								{:else if activeTransfers.length === 0}
									<TableRow>
										<TableCell colspan={4} class="h-24 text-center text-muted-foreground">
											No active transfers.
										</TableCell>
									</TableRow>
								{:else}
									{#each activeTransfers as transfer}
										<TableRow>
											<TableCell class="font-mono text-xs"
												>{transfer.hash.substring(0, 8)}...</TableCell
											>
											<TableCell class="max-w-[150px] truncate font-medium"
												>{transfer.title}</TableCell
											>
											<TableCell>
												<div class="h-2 w-full overflow-hidden rounded-full bg-secondary">
													<div class="h-full bg-primary" style="width: {transfer.progress}%"></div>
												</div>
												<span class="text-xs text-muted-foreground">{transfer.progress}%</span>
											</TableCell>
											<TableCell class="text-right text-xs">
												{transfer.speed > 0
													? (transfer.speed / 1024 / 1024).toFixed(1) + ' MB/s'
													: transfer.status}
											</TableCell>
										</TableRow>
									{/each}
								{/if}
							</TableBody>
						</Table>
					</CardContent>
				</Card>
			</div>
		</div>
	</main>
</div>
