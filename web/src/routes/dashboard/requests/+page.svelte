<script lang="ts">
	import * as Table from '$lib/components/ui/table';
	import { Button } from '$lib/components/ui/button';
	import { RefreshCw, CheckCircle2, Clock, XCircle } from 'lucide-svelte';
	import { toast } from 'svelte-sonner';
	import client from '$lib/api';
	import { onMount } from 'svelte';
	import * as Sidebar from '$lib/components/ui/sidebar/index.js';
	import { Separator } from '$lib/components/ui/separator';
	import * as Breadcrumb from '$lib/components/ui/breadcrumb/index.js';
	import { resolve } from '$app/paths';

	import type { components } from '$lib/api/api';

	let requests: any[] = $state([]);
	let isLoading = $state(true);

	async function loadRequests() {
		isLoading = true;
		try {
			const { data, error } = await client.GET('/api/v1/engine/seerr/requests', {});
			if (error) throw error;
			requests = (data?.results as any[]) || [];
		} catch (e) {
			toast.error('Failed to load Seerr requests. Check your Engine settings.');
			console.error(e);
		} finally {
			isLoading = false;
		}
	}

	onMount(() => {
		loadRequests();
	});
</script>

<svelte:head>
	<title>Seerr Requests - MediaManager</title>
	<meta content="View your active Seerr media request queue" name="description" />
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
					<Breadcrumb.Page>Seerr Requests</Breadcrumb.Page>
				</Breadcrumb.Item>
			</Breadcrumb.List>
		</Breadcrumb.Root>
	</div>
</header>

<main class="mx-auto flex w-full flex-1 flex-col gap-4 p-4 md:max-w-[80em]">
	<div class="my-6 flex items-center justify-between">
		<h1 class="scroll-m-20 text-4xl font-extrabold tracking-tight lg:text-5xl">Seerr Queue</h1>
		<Button variant="outline" size="icon" onclick={loadRequests} disabled={isLoading}>
			<RefreshCw class="h-4 w-4 {isLoading ? 'animate-spin' : ''}" />
			<span class="sr-only">Refresh</span>
		</Button>
	</div>

	<div class="overflow-hidden rounded-md border bg-card text-card-foreground shadow-sm">
		<div class="border-b p-4">
			<h3 class="leading-none font-semibold tracking-tight">Active Media Requests</h3>
			<p class="mt-2 text-sm text-muted-foreground">
				A live view of requests submitted through your connected Seerr instance.
			</p>
		</div>
		<Table.Root>
			<Table.Header>
				<Table.Row>
					<Table.Head>Title</Table.Head>
					<Table.Head>Type</Table.Head>
					<Table.Head>Requested By</Table.Head>
					<Table.Head>Status</Table.Head>
				</Table.Row>
			</Table.Header>
			<Table.Body>
				{#if isLoading}
					<Table.Row>
						<Table.Cell colspan={4} class="py-8 text-center text-muted-foreground"
							>Loading queue from Seerr...</Table.Cell
						>
					</Table.Row>
				{:else if requests.length === 0}
					<Table.Row>
						<Table.Cell colspan={4} class="py-8 text-center text-muted-foreground"
							>No active requests found.</Table.Cell
						>
					</Table.Row>
				{:else}
					{#each requests as req}
						<Table.Row>
							<Table.Cell class="font-medium">
								<div class="flex items-center gap-3">
									{#if req.media?.posterPath}
										<img
											src={`https://image.tmdb.org/t/p/w92${req.media.posterPath}`}
											alt="Poster"
											class="h-12 w-8 rounded-sm object-cover shadow-sm"
										/>
									{:else}
										<div class="flex h-12 w-8 items-center justify-center rounded-sm bg-muted">
											?
										</div>
									{/if}
									<span>{req.media?.title || req.media?.name || 'Unknown Title'}</span>
								</div>
							</Table.Cell>
							<Table.Cell class="capitalize">{req.type}</Table.Cell>
							<Table.Cell>
								<div class="flex items-center gap-2">
									{#if req.requestedBy?.avatar}
										<img src={req.requestedBy.avatar} alt="User" class="h-6 w-6 rounded-full" />
									{/if}
									<span
										>{req.requestedBy?.displayName ||
											req.requestedBy?.email ||
											'Unknown User'}</span
									>
								</div>
							</Table.Cell>
							<Table.Cell>
								<div
									class="inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-semibold
									{req.status === 2
										? 'border-transparent bg-primary/20 text-primary'
										: req.status === 3
											? 'border-transparent bg-destructive/20 text-destructive'
											: 'border-transparent bg-secondary text-secondary-foreground'}"
								>
									{#if req.status === 2}
										<CheckCircle2 class="h-3 w-3" /> Approved
									{:else if req.status === 3}
										<XCircle class="h-3 w-3" /> Declined
									{:else}
										<Clock class="h-3 w-3" /> Pending
									{/if}
								</div>
							</Table.Cell>
						</Table.Row>
					{/each}
				{/if}
			</Table.Body>
		</Table.Root>
	</div>
</main>
